<?php

namespace Yotpo\Core\Helper;

/**
 * Class Data for helper functions
 */
class Data
{
    /**
     * @param string $phone
     * @param string $country
     * @return string|null
     */
    public function formatPhoneNumber(string $phone, string $country)
    {
        if (!$phone) {
            return $phone;
        }

        $phoneCodes = PhoneCodes::$phoneCodes;
        $phoneCode = '';
        if ($phoneCodes) {
            if (array_key_exists($country, $phoneCodes)) {
                $phoneCode = $phoneCodes[$country];
            }
        }

        $phoneCodeChk =  (string) preg_replace("/[^0-9]/", "", $phoneCode);
        $phone =  (string) preg_replace("/[^0-9]/", "", $phone);
        $prefix = substr($phone, 0, 2);

        if (strpos($phone, $phoneCodeChk) === 0 || $prefix == '00') {
            $formattedPhone = '+'.$phone;
        } else {
            $formattedPhone = '+'.$phoneCode.$phone;
        }

        return $formattedPhone;
    }

    /**
     * Format date
     *
     * @param string|null $date
     * @return false|string|null
     */
    public function formatDate($date)
    {
        $time = $date ? strtotime($date) : null;
        return $time ? date("Y-m-d\TH:i:s\Z", $time) : null;
    }

    /**
     * Format date
     *
     * @param string|null $date
     * @return false|string|null
     */
    public function formatAdminConfigDate($date)
    {
        $time = $date ? strtotime($date) : null;
        return $time ? date("d M Y H:i:s", $time) : null;
    }

    /**
     * Format date
     *
     * @param string|null $date
     * @return false|string|null
     */
    public function formatOrderItemDate($date)
    {
        $time = $date ? strtotime($date) : null;
        return $time ? date("Y-m-d H:i:s", $time) : null;
    }
}
