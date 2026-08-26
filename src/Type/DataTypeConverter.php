<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Type;

use DateTime;
use DateTimeInterface;
use Throwable;

/**
 * Převod hodnot mezi PHP a Elasticsearch.
 *
 * Chování je záměrně shodné s původními metodami convertToDBDataTypes()/convertFromDBDataTypes()
 * na klientovi - ty na tuto třídu už jen delegují, aby zůstaly součástí veřejného API.
 * Do vnořených polí se ale zanořují ony samy, aby se jejich případné přepsání v potomkovi
 * uplatnilo na celý dokument, ne jen na jeho první úroveň.
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
            $data[$key] = is_array($value) ? $this->toDatabase($value) : $this->valueToDatabase($value);
        }

        return $data;
    }


    /**
     * Převede jednu hodnotu (ne pole) na hodnotu, které rozumí Elasticsearch.
     */
    public function valueToDatabase(mixed $value): mixed
    {
        // DateTimeImmutable je stejně dobré datum jako DateTime - putValuesIntoQuery()
        // pouští do podmínek obojí, takže se obojí musí i převést.
        return $value instanceof DateTimeInterface ? $value->format('c') : $value;
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
            $data[$key] = is_array($value) ? $this->fromDatabase($value) : $this->valueFromDatabase($value);
        }

        return $data;
    }


    /**
     * Převede jednu hodnotu (ne pole) z Elasticsearch na PHP typ.
     */
    public function valueFromDatabase(mixed $value): mixed
    {
        if (!is_string($value) || !self::looksLikeDate($value))
        {
            return $value;
        }

        return self::toDateTime($value) ?? $value;
    }


    private static function looksLikeDate(string $value): bool
    {
        return preg_match(self::DATE_PATTERN, $value) === 1
            || preg_match(self::DATE_TIME_PATTERN, $value) === 1;
    }


    /**
     * Vrátí datum, nebo null, pokud hodnota datum jen připomíná.
     *
     * Vzory pouštějí dál i nesmysly jako `2024-19-31` nebo `2024-02-30`; na prvním
     * `new DateTime()` spadne, druhé tiše přeteče do března. Ani jedno není datum,
     * které by knihovna zapsala, takže se taková hodnota vrací jako text.
     */
    private static function toDateTime(string $value): ?DateTime
    {
        try
        {
            $date = new DateTime($value);
        }
        catch(Throwable)
        {
            return null;
        }

        // Oba vzory začínají datem `YYYY-MM-DD`; když z něj vzniklo jiné, hodnota přetekla.
        return $date->format('Y-m-d') === substr($value, 0, 10) ? $date : null;
    }
}
