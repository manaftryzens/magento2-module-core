<?php
namespace Yotpo\Core\Model\Sync\Catalog;

use Magento\Catalog\Model\ResourceModel\Product\Collection;
use Magento\Framework\Exception\NoSuchEntityException;
use Magento\Sales\Model\Order;
use Yotpo\Core\Model\AbstractJobs;
use Yotpo\Core\Model\Config as CoreConfig;
use Yotpo\Core\Model\Sync\Catalog\Logger as YotpoCoreCatalogLogger;
use Magento\Store\Model\App\Emulation as AppEmulation;
use Yotpo\Core\Model\Sync\Catalog\Data as CatalogData;
use Yotpo\Core\Model\Api\Sync as CoreSync;
use Magento\Framework\App\ResourceConnection;
use Magento\Catalog\Model\ResourceModel\Product\CollectionFactory;
use Magento\Framework\Stdlib\DateTime\DateTime;
use Yotpo\Core\Model\Sync\Category\Processor\ProcessByProduct as CatgorySyncProcessor;
use Magento\Quote\Model\Quote;

/**
 * Class Processor - Process catalog sync
 */
class Processor extends AbstractJobs
{
    /**
     * @var CoreConfig
     */
    protected $coreConfig;

    /**
     * @var CatalogData
     */
    protected $catalogData;

    /**
     * @var CoreSync
     */
    protected $coreSync;

    /**
     * @var array<int, int>
     */
    protected $runStoreIds = [];

    /**
     * @var CollectionFactory
     */
    protected $collectionFactory;

    /**
     * @var null|int
     */
    protected $productSyncLimit = null;

    /**
     * @var DateTime
     */
    protected $dateTime;

    /**
     * @var YotpoResource
     */
    protected $yotpoResource;

    /**
     * @var YotpoCoreCatalogLogger
     */
    protected $yotpoCatalogLogger;

    /**
     * @var CatgorySyncProcessor
     */
    protected $catgorySyncProcessor;

    /**
     * @var string|null
     */
    protected $entityIdFieldValue;

    /**
     * @var boolean
     */
    private $normalSync = true;

    /**
     * Processor constructor.
     * @param AppEmulation $appEmulation
     * @param CoreConfig $coreConfig
     * @param YotpoCoreCatalogLogger $yotpoCatalogLogger
     * @param CatalogData $catalogData
     * @param CoreSync $coreSync
     * @param ResourceConnection $resourceConnection
     * @param CollectionFactory $collectionFactory
     * @param DateTime $dateTime
     * @param YotpoResource $yotpoResource,
     * @param CatgorySyncProcessor $catgorySyncProcessor
     */
    public function __construct(
        AppEmulation $appEmulation,
        CoreConfig $coreConfig,
        YotpoCoreCatalogLogger $yotpoCatalogLogger,
        CatalogData $catalogData,
        CoreSync $coreSync,
        ResourceConnection $resourceConnection,
        CollectionFactory $collectionFactory,
        DateTime $dateTime,
        YotpoResource $yotpoResource,
        CatgorySyncProcessor $catgorySyncProcessor
    ) {
        parent::__construct($appEmulation, $resourceConnection);
        $this->coreConfig = $coreConfig;
        $this->catalogData = $catalogData;
        $this->coreSync = $coreSync;
        $this->collectionFactory = $collectionFactory;
        $this->dateTime = $dateTime;
        $this->yotpoResource = $yotpoResource;
        $this->yotpoCatalogLogger = $yotpoCatalogLogger;
        $this->catgorySyncProcessor = $catgorySyncProcessor;
        $this->entityIdFieldValue = $this->coreConfig->getEavRowIdFieldName();
    }

    /**
     * Sync products during checkout
     * @param null|array <mixed> $unSyncedProductIds
     * @return bool
     */
    public function processCheckoutProducts($unSyncedProductIds)
    {
        $this->normalSync = false;
        try {
            $storeId = $this->coreConfig->getStoreId();
            $collection = $this->getCollectionForSync($unSyncedProductIds);
            $this->syncItems($collection->getItems(), $storeId);
            return true;
        } catch (NoSuchEntityException $e) {
            return false;
        }
    }
    /**
     * Process the Catalog Api
     * @param null|array <mixed> $unSyncedProductIds
     * @param Order|Quote $order
     * @return bool
     */
    public function process($unSyncedProductIds = null, $order = null)
    {
        try {
            $allStores = (array)$this->coreConfig->getAllStoreIds(false);
            $unSyncedStoreIds = [];
            foreach ($allStores as $storeId) {
                if (in_array($storeId, $this->runStoreIds)) {
                    continue;
                }
                $this->runStoreIds[] = $storeId;
                $this->emulateFrontendArea($storeId);
                try {
                    if (!$order && !$this->coreConfig->isCatalogSyncActive()) {
                        $this->yotpoCatalogLogger->info(
                            __('Product Sync - Disabled - Store ID: %1', $storeId)
                        );
                        $this->stopEnvironmentEmulation();
                        continue;
                    }
                    $this->productSyncLimit = $this->coreConfig->getConfig('product_sync_limit');
                    $this->yotpoCatalogLogger->info(
                        __('Product Sync - Start - Store ID: %1', $storeId)
                    );
                    $this->processDeleteData();
                    $this->processUnAssignData();
                    $collection = $this->getCollectionForSync($unSyncedProductIds);
                    $this->syncItems($collection->getItems(), $storeId);
                } catch (\Exception $e) {
                    $unSyncedStoreIds[] = $storeId;
                    $this->yotpoCatalogLogger->info(
                        __('Product Sync has stopped with exception :  %1, Store ID: %2', $e->getMessage(), $storeId)
                    );
                }
                $this->stopEnvironmentEmulation();
            }
            $this->stopEnvironmentEmulation();
            if ($order && in_array($order->getStoreId(), $unSyncedStoreIds)) {
                return false;
            } else {
                return true;
            }
        } catch (\Exception $e) {
            $this->yotpoCatalogLogger->info(
                __('Catalog sync::process() - Exception: ', [$e->getTraceAsString()])
            );
            return false;
        }
    }

    /**
     * @param array <mixed> $collectionItems
     * @param int $storeId
     * @param boolean $visibleVariants
     * @return void
     * @throws NoSuchEntityException
     */
    public function syncItems($collectionItems, $storeId, $visibleVariants = false)
    {
        if (count($collectionItems)) {
            $attributeId = $this->catalogData->getAttributeId(CoreConfig::CATALOG_SYNC_ATTR_CODE);
            $items = $this->manageSyncItems($collectionItems, $visibleVariants);
            $parentIds = $items['parent_ids'];
            $yotpoData = $items['yotpo_data'];
            $parentData = $items['parent_data'];
            $lastSyncTime = '';
            $sqlData = $sqlDataIntTable = [];
            $externalIds = [];
            $visibleVariantsData = $visibleVariants ? [] : $items['visible_variants'];
            $visibleVariantsDataValues = array_values($visibleVariantsData);
            $visibleVariantsDataKeys = array_keys($visibleVariantsData);
            foreach ($items['sync_data'] as $itemId => $itemData) {
                $rowId = $itemData['row_id'];
                unset($itemData['row_id']);
                if ($yotpoData
                    && array_key_exists($itemId, $yotpoData)
                    && !$this->coreConfig->canResync($yotpoData[$itemId]['response_code'], $yotpoData[$itemId])
                ) {
                    $tempSqlDataIntTable = [
                        'attribute_id' => $attributeId,
                        'store_id' => $storeId,
                        'value' => 1,
                        $this->entityIdFieldValue => $rowId
                    ];
                    $sqlDataIntTable[] = $tempSqlDataIntTable;
                    continue;
                }
                $apiParam = $this->getApiParams($itemId, $yotpoData, $parentIds, $parentData, $visibleVariants);
                if (!$apiParam) {
                    $parentProductId = $parentIds[$itemId] ?? 0;
                    if ($parentProductId) {
                        continue;
                    }
                }
                $this->yotpoCatalogLogger->info(
                    __('Data ready to sync - Method: %1 - Store ID: %2', $apiParam['method'], $storeId)
                );
                $response = $this->processRequest($apiParam, $itemData);
                $lastSyncTime = $this->getCurrentTime();
                $yotpoIdKey = $visibleVariants ? 'visible_variant_yotpo_id' : 'yotpo_id';
                $tempSqlArray = [
                    'product_id' => $itemId,
                    $yotpoIdKey => $apiParam['yotpo_id'] ?: 0,
                    'store_id' => $storeId,
                    'synced_to_yotpo' => $lastSyncTime,
                    'response_code' => $response->getData('status'),
                    'sync_status' => 1
                ];
                if (!$visibleVariants) {
                    $tempSqlArray['yotpo_id_parent']  = $apiParam['yotpo_id_parent'] ?: 0;
                }
                if ($this->coreConfig->canUpdateCustomAttributeForProducts($tempSqlArray['response_code'])) {
                    $tempSqlDataIntTable = [
                        'attribute_id' => $attributeId,
                        'store_id' => $storeId,
                        'value' => 1,
                        $this->entityIdFieldValue => $rowId
                    ];
                    $sqlDataIntTable[] = $tempSqlDataIntTable;
                }
                $returnResponse = $this->processResponse(
                    $apiParam,
                    $response,
                    $tempSqlArray,
                    $itemData,
                    $externalIds,
                    $visibleVariants
                );
                $tempSqlArray = $returnResponse['temp_sql'];
                $externalIds = $returnResponse['external_id'];

                //push to parentData array if parent product is
                // being the part of current collection
                if (!$visibleVariants) {
                    $parentData = $this->pushParentData((int)$itemId, $tempSqlArray, $parentData, $parentIds);
                }
                $sqlData[] = $tempSqlArray;
            }
            $dataToSent = [];
            if (count($sqlData)) {
                $this->insertOnDuplicate(
                    'yotpo_product_sync',
                    $sqlData
                );
                $dataToSent = array_merge($dataToSent, $this->catalogData->filterDataForCatSync($sqlData));
            }

            if (count($sqlDataIntTable) && $this->normalSync) {
                $this->insertOnDuplicate(
                    'catalog_product_entity_int',
                    $sqlDataIntTable
                );
            }

            if ($externalIds) {
                $yotpoExistingProducts = $this->processExistData(
                    $externalIds,
                    $parentData,
                    $parentIds,
                    $visibleVariants
                );
                if ($yotpoExistingProducts && $this->normalSync) {
                    $dataToSent = array_merge(
                        $dataToSent,
                        $this->catalogData->filterDataForCatSync($yotpoExistingProducts)
                    );
                }
            }
            if ($this->normalSync) {
                $this->coreConfig->saveConfig('catalog_last_sync_time', $lastSyncTime);
                $dataForCategorySync = [];
                if ($dataToSent && !$visibleVariants) {
                    $dataForCategorySync = $this->getProductsForCategorySync(
                        $dataToSent,
                        $collectionItems,
                        $dataForCategorySync
                    );
                }
                if (count($dataForCategorySync) > 0) {
                    $this->catgorySyncProcessor->process($dataForCategorySync);
                }
            }
            if ($visibleVariantsDataValues && !$visibleVariants) {
                $this->syncItems($visibleVariantsDataValues, $storeId, true);
            }
        } else {
            $this->yotpoCatalogLogger->info(
                __('Product Sync complete : No Data, Store ID: %1', $storeId)
            );
        }
    }

    /**
     * Trigger API
     * @param array<string, string> $params
     * @param mixed $data
     * @return mixed
     * @throws NoSuchEntityException
     */
    protected function processRequest($params, $data)
    {
        switch ($params['method']) {
            case $this->coreConfig->getProductSyncMethod('createProduct'):
            default:
                $data = ['product' => $data, 'entityLog' => 'catalog'];
                $response = $this->coreSync->sync('POST', $params['url'], $data);
                break;
            case $this->coreConfig->getProductSyncMethod('updateProduct'):
            case $this->coreConfig->getProductSyncMethod('deleteProduct'):
            case $this->coreConfig->getProductSyncMethod('unassignProduct'):
                $data = ['product' => $data, 'entityLog' => 'catalog'];
                $response = $this->coreSync->sync('PATCH', $params['url'], $data);
                break;
            case $this->coreConfig->getProductSyncMethod('createProductVariant'):
                $data = ['variant' => $data, 'entityLog' => 'catalog'];
                $response = $this->coreSync->sync('POST', $params['url'], $data);
                break;
            case $this->coreConfig->getProductSyncMethod('updateProductVariant'):
            case $this->coreConfig->getProductSyncMethod('deleteProductVariant'):
            case $this->coreConfig->getProductSyncMethod('unassignProductVariant'):
                $data = ['variant' => $data, 'entityLog' => 'catalog'];
                $response = $this->coreSync->sync('PATCH', $params['url'], $data);
                break;
        }

        return $response;
    }

    /**
     * Handle response
     * @param array<string, int|string> $apiParam
     * @param mixed $response
     * @param array<string, string|int> $tempSqlArray
     * @param mixed $data
     * @param array<int, int> $externalIds
     * @param boolean $visibleVariants
     * @return array<string, mixed>
     * @throws NoSuchEntityException
     */
    protected function processResponse(
        $apiParam,
        $response,
        $tempSqlArray,
        $data,
        $externalIds = [],
        $visibleVariants = false
    ) {
        $storeId = $this->coreConfig->getStoreId();
        switch ($apiParam['method']) {
            case $this->coreConfig->getProductSyncMethod('createProduct'):
            case $this->coreConfig->getProductSyncMethod('createProductVariant'):
            default:
                if ($visibleVariants) {
                    $yotpoIdkey = 'visible_variant_yotpo_id';
                } else {
                    $yotpoIdkey = 'yotpo_id';
                }
                if ($response->getData('is_success')) {
                    $tempSqlArray[$yotpoIdkey] = $this->getYotpoIdFromResponse($response, $apiParam['method']);
                    $this->writeSuccessLog($apiParam['method'], $storeId, $data);
                } else {
                    if ($response->getStatus() == '409') {
                        $externalIds[] = $data['external_id'];
                    }
                    $tempSqlArray[$yotpoIdkey] = 0;
                    $this->writeFailedLog($apiParam['method'], $storeId, []);
                }
                break;
            case $this->coreConfig->getProductSyncMethod('updateProduct'):
            case $this->coreConfig->getProductSyncMethod('updateProductVariant'):
            case $this->coreConfig->getProductSyncMethod('deleteProduct'):
            case $this->coreConfig->getProductSyncMethod('deleteProductVariant'):
            case $this->coreConfig->getProductSyncMethod('unassignProduct'):
            case $this->coreConfig->getProductSyncMethod('unassignProductVariant'):
                if ($response->getData('is_success')) {
                    $this->writeSuccessLog($apiParam['method'], $storeId, $data);
                    if ($apiParam['method'] === $this->coreConfig->getProductSyncMethod('deleteProduct')
                        || $apiParam['method'] === $this->coreConfig->getProductSyncMethod('deleteProductVariant')) {
                        $tempSqlArray['is_deleted_at_yotpo'] = 1;
                    }
                    if ($apiParam['method'] === $this->coreConfig->getProductSyncMethod('unassignProduct')
                        || $apiParam['method'] === $this->coreConfig->getProductSyncMethod('unassignProductVariant')) {
                        $tempSqlArray['yotpo_id_unassign'] = 0;
                    }
                } else {
                    $this->writeFailedLog($apiParam['method'], $storeId, []);
                }
                break;
        }

        return [
            'temp_sql' => $tempSqlArray,
            'external_id' => $externalIds
        ];
    }

    /**
     * Success Log
     * @param string|int $method
     * @param int $storeId
     * @param mixed $data
     * @return void
     */
    protected function writeSuccessLog($method, $storeId, $data)
    {
        $this->yotpoCatalogLogger->info(
            __('%1 API ran successfully - Store Id: %2', $method, $storeId),
            $data
        );
    }

    /**
     * Failed Log
     * @param string|int $method
     * @param int $storeId
     * @param mixed $data
     * @return void
     */
    protected function writeFailedLog($method, $storeId, $data)
    {
        $this->yotpoCatalogLogger->info(
            __('%1 API Failed - Store Id: %2', $method, $storeId),
            $data
        );
    }

    /**
     * Get yotpo_id from response
     * @param mixed $response
     * @param string|int $method
     * @return string|int
     */
    private function getYotpoIdFromResponse($response, $method)
    {
        $array = [
            $this->coreConfig->getProductSyncMethod('createProduct') => 'product',
            $this->coreConfig->getProductSyncMethod('createProductVariant') => 'variant'
        ];
        return $response->getData('response')[$array[$method]]['yotpo_id'];
    }

    /**
     * Notify to API that the product is deleted from Magento Catalog
     * @return void
     * @throws NoSuchEntityException
     */
    protected function processDeleteData()
    {
        $storeId = $this->coreConfig->getStoreId();
        $data = $this->getToDeleteCollection($storeId);
        $dataCount = count($data);
        if ($dataCount > 0) {
            $sqlData = [];
            foreach ($data as $itemId => $itemData) {
                $tempDeleteQry = [
                    'product_id' => $itemId,
                    'is_deleted_at_yotpo' => 0,
                    'store_id' => $storeId
                ];
                if (!$itemData['yotpo_id']) {
                    $tempDeleteQry['is_deleted_at_yotpo'] = 1;
                    $sqlData[] = $tempDeleteQry;
                    continue;
                }
                $params = $this->getDeleteApiParams($itemData, 'yotpo_id');
                $itemData = ['is_discontinued' => true];
                $response = $this->processRequest($params, $itemData);
                $returnResponse = $this->processResponse($params, $response, $tempDeleteQry, $itemData);
                $sqlData[] = $returnResponse['temp_sql'];
            }

            $this->insertOnDuplicate(
                'yotpo_product_sync',
                $sqlData
            );

            $this->updateProductSyncLimit($dataCount);
        }
    }

    /**
     * Collection for fetching the data to delete
     * @param int $storeId
     * @return array<int, array<string, string|int>>
     */
    protected function getToDeleteCollection($storeId)
    {
        return $this->yotpoResource->getToDeleteCollection($storeId, (int) $this->productSyncLimit);
    }

    /**
     * Collection for fetching the data to delete
     * @param int $storeId
     * @return array<int, array<string, string|int>>
     */
    protected function getUnAssignedCollection($storeId)
    {
        return $this->yotpoResource->getUnAssignedCollection($storeId, (int) $this->productSyncLimit);
    }

    /**
     * @return void
     * @throws NoSuchEntityException
     */
    protected function processUnAssignData()
    {
        $storeId = $this->coreConfig->getStoreId();
        $data = $this->getUnAssignedCollection($storeId);
        $dataCount = count($data);
        if ($dataCount > 0) {
            $sqlData = [];
            foreach ($data as $itemId => $itemData) {
                $tempDeleteQry = [
                    'product_id' => $itemId,
                    'is_deleted_at_yotpo' => 0,
                    'yotpo_id_unassign' => $itemData['yotpo_id_unassign'],
                    'store_id' => $storeId
                ];
                $params = $this->getDeleteApiParams($itemData, 'yotpo_id_unassign');

                $itemData = ['is_discontinued' => true];
                $response = $this->processRequest($params, $itemData);
                $returnResponse = $this->processResponse($params, $response, $tempDeleteQry, $itemData);
                $sqlData[] = $returnResponse['temp_sql'];
            }

            $this->insertOnDuplicate(
                'yotpo_product_sync',
                $sqlData
            );

            $this->updateProductSyncLimit($dataCount);
        }
    }

    /**
     * Prepare collection query to fetch data
     * @param array<mixed> $unSyncedProductIds
     * @return Collection<mixed>
     */
    protected function getCollectionForSync($unSyncedProductIds = null): Collection
    {
        $collection = $this->collectionFactory->create();
        $collection->addAttributeToSelect('*');
        if (!$unSyncedProductIds) {
            $collection->addAttributeToFilter(
                [
                    ['attribute' => CoreConfig::CATALOG_SYNC_ATTR_CODE, 'null' => true],
                    ['attribute' => CoreConfig::CATALOG_SYNC_ATTR_CODE, 'eq' => '0'],
                ]
            );
        }
        if ($unSyncedProductIds) {
            $collection->addFieldToFilter('entity_id', ['in' => $unSyncedProductIds]);
        }
        $collection->addUrlRewrite();
        $collection->addStoreFilter();
        $collection->getSelect()->order('type_id');
        $collection->setFlag('has_stock_status_filter', false);
        $collection->getSelect()->limit($this->productSyncLimit);
        return $collection;
    }

    /**
     * @param array <mixed> $items
     * @param boolean $visibleVariants
     * @return array <mixed>
     * @throws NoSuchEntityException
     */
    protected function manageSyncItems($items, $visibleVariants = false): array
    {
        return $this->catalogData->manageSyncItems($items, $visibleVariants);
    }

    /**
     * Get current time
     *
     * @return string
     */
    private function getCurrentTime()
    {
        return $this->dateTime->gmtDate();
    }

    /**
     * Get API End URL and API Method
     *
     * @param int|string $productId
     * @param array<int, array> $yotpoData
     * @param array<int, int> $parentIds
     * @param array<int|string, mixed> $parentData
     * @param boolean $visibleVariants
     * @return array<string, string>
     */
    private function getApiParams(
        $productId,
        array $yotpoData,
        array $parentIds,
        array $parentData,
        $visibleVariants = false
    ) {
        $apiUrl = $this->coreConfig->getEndpoint('products');
        $method = $this->coreConfig->getProductSyncMethod('createProduct');
        $yotpoIdParent = $yotpoId = '';

        if (count($parentIds) && !$visibleVariants) {
            if (isset($parentIds[$productId])
                && isset($parentData[$parentIds[$productId]])
                && isset($parentData[$parentIds[$productId]]['yotpo_id'])
                && $yotpoIdParent = $parentData[$parentIds[$productId]]['yotpo_id']) {

                $method = $this->coreConfig->getProductSyncMethod('createProductVariant');
                $apiUrl = $this->coreConfig->getEndpoint(
                    'variant',
                    ['{yotpo_product_id}'],
                    [$yotpoIdParent]
                );
            } elseif (isset($parentIds[$productId])) {
                return [];
            }
        }

        $yotpoIdKey = $visibleVariants ? 'visible_variant_yotpo_id' : 'yotpo_id';
        if (count($yotpoData)) {
            if (isset($yotpoData[$productId])
                && isset($yotpoData[$productId][$yotpoIdKey])
                ) {

                $yotpoId = $yotpoData[$productId][$yotpoIdKey] ;

                if ($yotpoId && $method ==  $this->coreConfig->getProductSyncMethod('createProduct')) {
                    $apiUrl = $this->coreConfig->getEndpoint(
                        'updateProduct',
                        ['{yotpo_product_id}'],
                        [$yotpoId]
                    );
                    $method = $this->coreConfig->getProductSyncMethod('updateProduct');

                } elseif ($yotpoId && $method ==  $this->coreConfig->getProductSyncMethod('createProductVariant')) {
                    $apiUrl = $this->coreConfig->getEndpoint(
                        'updateVariant',
                        ['{yotpo_product_id}','{yotpo_variant_id}'],
                        [$yotpoIdParent, $yotpoId]
                    );
                    $method = $this->coreConfig->getProductSyncMethod('updateProductVariant');
                }

            }
        }

        return [
            'url' => $apiUrl,
            'method' => $method,
            'yotpo_id' => $yotpoId,
            'yotpo_id_parent' => $yotpoIdParent
        ];
    }

    /**
     * @param array<string, int|string> $data
     * @param string $key
     * @return array<string, mixed>
     */
    private function getDeleteApiParams($data, $key)
    {
        if ($variantId = $data['yotpo_id_parent']) {
            $apiUrl = $this->coreConfig->getEndpoint(
                'updateVariant',
                ['{yotpo_product_id}','{yotpo_variant_id}'],
                [$variantId, $data[$key]]
            );
            if ($key === 'yotpo_id') {
                $method = $this->coreConfig->getProductSyncMethod('deleteProductVariant');
            } else {
                $method = $this->coreConfig->getProductSyncMethod('unassignProductVariant');
            }
        } else {
            $apiUrl = $this->coreConfig->getEndpoint(
                'updateProduct',
                ['{yotpo_product_id}'],
                [$data[$key]]
            );
            if ($key === 'yotpo_id') {
                $method = $this->coreConfig->getProductSyncMethod('deleteProduct');
            } else {
                $method = $this->coreConfig->getProductSyncMethod('unassignProduct');
            }
        }

        return ['url' => $apiUrl, 'method' => $method, $key => $data[$key]];
    }

    /**
     * Push Yotpo Id to Parent data
     * @param int $productId
     * @param array<string, int|string> $tempSqlArray
     * @param array<int|string, mixed> $parentData
     * @param array<int, int> $parentIds
     * @return array<int|string, mixed>
     */
    protected function pushParentData($productId, $tempSqlArray, $parentData, $parentIds)
    {
        $yotpoId = 0;
        if (isset($tempSqlArray['yotpo_id'])
            && $tempSqlArray['yotpo_id']) {
            if (!isset($parentData[$productId])) {
                $parentId = $this->findParentId($productId, $parentIds);
                if ($parentId) {
                    $yotpoId = $tempSqlArray['yotpo_id'];
                }
            }
        }
        if ($yotpoId) {
            $parentData[$productId] = [
                'product_id' => $productId,
                'yotpo_id' => $yotpoId
            ];
        }

        return $parentData;
    }

    /**
     * Find it parent ID is exist in the synced data
     * @param int $productId
     * @param array<int, int> $parentIds
     * @return false|int|string
     */
    protected function findParentId($productId, $parentIds)
    {
        return array_search($productId, $parentIds);
    }

    /**
     * Calculate the remaining limit
     * @param int $delta
     * @return void
     */
    public function updateProductSyncLimit($delta)
    {
        $this->productSyncLimit = $this->productSyncLimit - $delta;
    }

    /**
     * Fetch Existing data and update the product_sync table
     *
     * @param array<int, int|string> $externalIds
     * @param array<int|string, mixed> $parentData
     * @param array<int, int> $parentIds
     * @param boolean $visibleVariants
     * @return array<mixed>
     * @throws NoSuchEntityException
     */
    protected function processExistData(
        array $externalIds,
        array $parentData,
        array $parentIds,
        $visibleVariants = false
    ) {
        $sqlData = $filters = [];
        if (count($externalIds) > 0) {
            foreach ($externalIds as $externalId) {
                if (isset($parentIds[$externalId]) && $parentIds[$externalId] && !$visibleVariants) {
                    if (isset($parentData[$parentIds[$externalId]]) &&
                        isset($parentData[$parentIds[$externalId]]['yotpo_id']) &&
                        $parentYotpoId = $parentData[$parentIds[$externalId]]['yotpo_id']) {
                        $filters['variants'][$parentYotpoId][] = $externalId;
                    } else {
                        $filters['products'][] = $externalId;
                    }
                } else {
                    $filters['products'][] = $externalId;
                }
            }
            foreach ($filters as $key => $filter) {
                if (($key === 'products') && (count($filter) > 0)) {
                    $requestIds = implode(',', $filter);
                    $url = $this->coreConfig->getEndpoint('products');
                    $sqlData = $this->existDataRequest($url, $requestIds, $sqlData, 'products', $visibleVariants);
                }

                if (($key === 'variants') && (count($filter) > 0)) {
                    foreach ($filter as $parent => $variant) {

                        $requestIds = implode(',', (array)$variant);
                        $url = $this->coreConfig->getEndpoint(
                            'variant',
                            ['{yotpo_product_id}'],
                            [$parent]
                        );
                        $sqlData = $this->existDataRequest($url, $requestIds, $sqlData, 'variants', $visibleVariants);
                    }
                }
            }
            if (count($sqlData)) {
                $this->insertOnDuplicate(
                    'yotpo_product_sync',
                    $sqlData
                );
            }
        }
        return $sqlData;
    }

    /**
     * Send GET request to Yotpo to fetch the existing data details
     *
     * @param string $url
     * @param string $requestIds
     * @param array<int, string|int> $sqlData
     * @param string $type
     * @param boolean $visibleVariants
     * @return array<int, mixed>
     * @throws NoSuchEntityException
     */
    protected function existDataRequest($url, $requestIds, $sqlData, $type = 'products', $visibleVariants = false)
    {
        $data = ['external_ids' => $requestIds, 'entityLog' => 'catalog'];
        $response = $this->coreSync->sync('GET', $url, $data);
        if ($type == 'variants') {
            $products = $response->getResponse()['variants'];
        } else {
            $products = $response->getResponse()['products'];
        }

        if ($products) {
            foreach ($products as $product) {
                $parentId = 0;
                if (isset($product['yotpo_product_id']) && $product['yotpo_product_id']) {
                    $parentId = $product['yotpo_product_id'];
                }
                $yotpoIdKey = $visibleVariants ? 'visible_variant_yotpo_id' : 'yotpo_id';
                $sqlData[] = [
                    'product_id' => $product['external_id'],
                    'store_id' => $this->coreConfig->getStoreId(),
                    $yotpoIdKey => $product['yotpo_id'],
                    'yotpo_id_parent' => $parentId,
                    'response_code' => '200'
                ];
            }
        }

        return $sqlData;
    }

    /**
     * Prepare products to sync their category data
     * @param array<mixed> $data
     * @param array<mixed> $collectionItems
     * @param array<mixed> $dataForCategorySync
     * @return array<mixed>
     */
    protected function getProductsForCategorySync($data, $collectionItems, array $dataForCategorySync): array
    {
        foreach ($data as $dataItem) {
            foreach ($collectionItems as $item) {
                if ($item->getId() == $dataItem['product_id']) {
                    $dataForCategorySync[$dataItem['yotpo_id']] = $item;
                    break;
                }
            }
        }
        return $dataForCategorySync;
    }
}
