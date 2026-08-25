<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Type;

use DateTime;
use Throwable;

/**
 * Převod hodnot mezi PHP a Elasticsearch.
 *
 * Chování je záměrně shodné s původními metodami convertToDBDataTypes()/convertFromDBDataTypes()
 * na klientovi - ty na tuto třídu už jen delegují, aby zůstaly součástí veřejného API.
 */
final class DataTypeConverter
{
    /** Samotné datum, např. `2024-01-31`. */
    private const DATE_PATTERN = '/^[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]$/';

    /**
     * Datum s časem, např. `2024-01-31T12:00` nebo `2024-01-31T12:00:00+01:00`.
     * Vzor je ukotvený - text, který datum jen obsahuje (poznámka, popis), není datum.
     */
    private const DATE_TIME_PATTERN = '/^[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]'
        . 'T[0-2][0-9]:[0-5][0-9](:[0-5][0-9](\.[0-9]+)?)?(Z|[+-][0-2][0-9]:?[0-5][0-9])?$/';


    /**
     * Převede PHP hodnoty na hodnoty, kterým rozumí Elasticsearch.
     * @param mixed[] $data
     * @return mixed[]
     */
    public function toDatabase(array $data): array
    {
        foreach ($data as $key => $value)
        {
            if ($value instanceof DateTime)
            {
                $data[$key] = $value->format('c');
            }
            elseif (is_array($value))
            {
                $data[$key] = $this->toDatabase($value);
            }
        }

        return $data;
    }


    /**
     * Převede hodnoty z Elasticsearch zpět na PHP typy.
     * @param mixed[] $data
     * @return mixed[]
     * @throws Throwable
     */
    public function fromDatabase(array $data): array
    {
        foreach ($data as $key => $value)
        {
            if (is_array($value))
            {
                $data[$key] = $this->fromDatabase($value);
            }
            elseif (is_string($value) && self::looksLikeDate($value))
            {
                $data[$key] = new DateTime($value);
            }
        }

        return $data;
    }


    private static function looksLikeDate(string $value): bool
    {
        return preg_match(self::DATE_PATTERN, $value) === 1
            || preg_match(self::DATE_TIME_PATTERN, $value) === 1;
    }
}
