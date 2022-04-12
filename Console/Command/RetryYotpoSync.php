<?php

namespace Yotpo\Core\Console\Command;

use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\Exception\NoSuchEntityException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Input\InputOption;
use Yotpo\Core\Model\Sync\Customers\Processor as CustomersProcessor;
use Yotpo\Core\Model\Sync\Orders\Processor as OrdersProcessor;
use Yotpo\Core\Model\Sync\Category\Processor\ProcessByCategory as CategoryProcessor;
use Yotpo\Core\Model\Sync\Catalog\Processor as CatalogProcessor;
use Magento\Framework\App\State as AppState;

/**
 * Class RetryCustomersSync - Manage Yotpo Resync
 */
class RetryYotpoSync extends Command
{
    const YOTPO_ENTITY              = 'entity';
    const YOTPO_ENTITY_ALL          = 'all';
    const YOTPO_ENTITY_CUSTOMERS    = 'customer';
    const YOTPO_ENTITY_ORDERS       = 'order';
    const YOTPO_ENTITY_CATEGORY     = 'category';
    const YOTPO_ENTITY_PRODUCT      = 'product';
    const YOTPO_ENTITY_CATALOG      = 'catalog';

    /**
     * @var string[]
     */
    protected $yotpoCoreEntities = ['order', 'category', 'product', 'catalog', 'all'];

    /**
     * @var CustomersProcessor
     */
    protected $customersProcessor;

    /**
     * @var OrdersProcessor
     */
    protected $ordersProcessor;

    /**
     * @var CategoryProcessor
     */
    protected $categoryProcessor;

    /**
     * @var CatalogProcessor
     */
    protected $catalogProcessor;

    /**
     * @var AppState
     */
    protected $appState;

    /**
     * RetryYotpoSync constructor.
     * @param CustomersProcessor $customersProcessor
     * @param OrdersProcessor $ordersProcessor
     * @param CategoryProcessor $categoryProcessor
     * @param CatalogProcessor $catalogProcessor
     * @param AppState $appState
     * @param string|null $name
     */
    public function __construct(
        CustomersProcessor $customersProcessor,
        OrdersProcessor $ordersProcessor,
        CategoryProcessor $categoryProcessor,
        CatalogProcessor $catalogProcessor,
        AppState $appState,
        string $name = null
    ) {
        $this->customersProcessor = $customersProcessor;
        $this->ordersProcessor = $ordersProcessor;
        $this->categoryProcessor = $categoryProcessor;
        $this->catalogProcessor = $catalogProcessor;
        $this->appState = $appState;
        parent::__construct($name);
    }

    /**
     * Configures the current command.
     *
     * @return void
     */
    protected function configure()
    {
        $options = [
            new InputOption(
                self::YOTPO_ENTITY,
                null,
                InputOption::VALUE_REQUIRED,
                'Entity'
            ),
            new InputOption(
                self::YOTPO_ENTITY_ALL,
                null,
                InputOption::VALUE_NONE,
                'All Entities'
            )
        ];

        $this->setName('yotpo:resync')
            ->setDescription('Retry Sync')
            ->setDefinition($options);

        parent::configure();
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return $this|int
     * @throws LocalizedException
     * @throws NoSuchEntityException
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->setAreaCode();

        $isAllOption = $input->getOption(self::YOTPO_ENTITY_ALL);
        if ($isAllOption) {
            $this->resyncAllEntities($output);
        } else {
           $yotpoEntityInput = $input->getOption(self::YOTPO_ENTITY);
            if (!$yotpoEntityInput) {
                return $this;
            }
            if (!is_array($yotpoEntityInput)) {
                $yotpoEntityInput = [$yotpoEntityInput];
            }
            $this->retryYotpoSync($yotpoEntityInput, $output);
        }
        return $this;
    }

    /**
     * @return void
     * @throws LocalizedException
     */
    public function setAreaCode()
    {
        try {
            $this->appState->getAreaCode();
        } catch (LocalizedException $e) {
            $this->appState->setAreaCode(\Magento\Framework\App\Area::AREA_CRONTAB);
        }
    }

    /**
     * Resync all entities
     * @param OutputInterface $output
     * @throws LocalizedException
     * @throws NoSuchEntityException
     * @return void
     */
    public function resyncAllEntities(OutputInterface $output)
    {
        $this->retryCatalogSync();
        if (method_exists($this->customersProcessor, 'retryCustomersSync')) {
            $this->customersProcessor->retryCustomersSync();
        } else {
            $output->writeln('SmsBump module is not installed to process customers.');
        }
        $this->ordersProcessor->retryOrdersSync();
    }

    /**
     * Retry product and category sync
     *
     * @throws LocalizedException
     * @throws NoSuchEntityException
     * @return void
     */
    public function retryCatalogSync()
    {
        $this->catalogProcessor->retryProductSync();
        $this->categoryProcessor->retryCategorySync();
    }

    /**
     * @param array <bool|string|null> $yotpoEntityInput
     * @param OutputInterface $output
     * @return void
     */
    public function retryYotpoSync($yotpoEntityInput, OutputInterface $output)
    {
        foreach ($yotpoEntityInput as $yotpoEntity) {
            switch ($yotpoEntity) {
                case self::YOTPO_ENTITY_CATEGORY:
                    $this->categoryProcessor->retryCategorySync();
                    break;
                case self::YOTPO_ENTITY_PRODUCT:
                    $this->catalogProcessor->retryProductSync();
                    break;
                case self::YOTPO_ENTITY_CATALOG:
                    $this->retryCatalogSync();
                    break;
                case self::YOTPO_ENTITY_CUSTOMERS:
                    if (method_exists($this->customersProcessor, 'retryCustomersSync')) {
                        $this->customersProcessor->retryCustomersSync();
                        $output->writeln('Entity ' . $yotpoEntity . ' resync completed');
                    } else {
                        $output->writeln('SmsBump module is not installed to process customers.');
                    }
                    break;
                case self::YOTPO_ENTITY_ORDERS:
                    $this->ordersProcessor->retryOrdersSync();
                    break;
                case self::YOTPO_ENTITY_ALL:
                    $this->resyncAllEntities($output);
                    break;
                default:
                    $output->writeln('Yotpo Resync');
                    break;
            }
            if ($yotpoEntity !== self::YOTPO_ENTITY_CUSTOMERS) {
                if (in_array($yotpoEntity, $this->yotpoCoreEntities)) {
                    $output->writeln('Entity - ' . $yotpoEntity . ' resync completed');
                } else {
                    $output->writeln('Entity - ' . $yotpoEntity . ' does not exist');
                }
            }
        }
    }
}
