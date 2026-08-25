<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use Closure;
use Hovjacky\NoSQL\NotImplementedException;
use Throwable;

/**
 * Převede odpověď z Elasticsearch na řádky, které vrací findBy().
 *
 * Odpověď je dynamický JSON, proto je `$response` záměrně netypované `array` - typování
 * na `array<mixed>` by u přístupu ke vnořeným klíčům spouštělo chyby o přístupu na `mixed`.
 */
final class SearchResultMapper
{
    /**
     * @param string $bucketKey klíč, pod kterým se do výsledku přidají surová data bucketu
     * @param int $groupLimit kolik záznamů se vrací pro jednu GROUP BY hodnotu
     */
    public function __construct(
        private readonly string $bucketKey,
        private readonly int $groupLimit,
    )
    {
    }


    /**
     * @param Closure(mixed[]): mixed[] $rowConverter převod hodnot z databázových typů na PHP
     * @return array<int, array<string, mixed>>|int
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    public function map(array $response, FindByParams $params, Closure $rowConverter): array|int
    {
        if ($params->groupBy !== null)
        {
            return $this->mapBuckets($response, $params, $rowConverter);
        }

        if ($params->aggregation !== [])
        {
            return $this->mapAggregations($response, $params, $rowConverter);
        }

        return $this->mapHits($response, $params, $rowConverter);
    }


    /**
     * Výsledky seskupené přes GROUP BY, tj. buckets agregace `group_by`.
     * @param Closure(mixed[]): mixed[] $rowConverter
     * @return array<int, array<string, mixed>>|int
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    private function mapBuckets(array $response, FindByParams $params, Closure $rowConverter): array|int
    {
        $buckets = $response['aggregations']['group_by']['buckets'];

        // Pokud zjišťujeme pouze počet záznamů, zajímá nás počet buckets
        if ($params->count)
        {
            return count($buckets);
        }

        $results = [];

        foreach ($buckets as $bucket)
        {
            $row = $this->bucketToRow($bucket, $params);

            $row[$this->bucketKey] = $bucket;

            $results[] = $rowConverter($row);
        }

        return $params->offset !== null ? array_slice($results, $params->offset) : $results;
    }


    /**
     * @return array<string, mixed>
     * @phpstan-ignore missingType.iterableValue
     */
    private function bucketToRow(array $bucket, FindByParams $params): array
    {
        if (!empty($bucket['results']))
        {
            if ($this->groupLimit !== 1)
            {
                throw new NotImplementedException(
                    'Vracení jiného počtu výsledků než 1 pro skupinu (v GROUP BY) je třeba doimplementovat.',
                );
            }

            // Výsledky s top_hits
            $row = $bucket['results']['hits']['hits'][0]['_source'];
        }
        else
        {
            $row = [$params->groupBy => $bucket['key'], 'count' => $bucket['doc_count']];
        }

        foreach (self::aggregationResultKeys($params) as $resultKey)
        {
            $row[$resultKey] = $bucket[$resultKey]['value'];
        }

        return $row;
    }


    /**
     * Agregace bez GROUP BY - výsledkem je jediný řádek s hodnotami agregací.
     * @param Closure(mixed[]): mixed[] $rowConverter
     * @return array<int, array<string, mixed>>
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    private function mapAggregations(array $response, FindByParams $params, Closure $rowConverter): array
    {
        $row = [];

        foreach (self::aggregationResultKeys($params) as $resultKey)
        {
            $row[$resultKey] = $response['aggregations'][$resultKey]['value'];
        }

        return [$rowConverter($row)];
    }


    /**
     * Běžné výsledky vyhledávání.
     * @param Closure(mixed[]): mixed[] $rowConverter
     * @return array<int, array<string, mixed>>
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    private function mapHits(array $response, FindByParams $params, Closure $rowConverter): array
    {
        // `id` není součástí `_source`, musí se doplnit z `_id`.
        $addId = $params->fields !== null && in_array('id', $params->fields);

        $results = [];

        foreach ($response['hits']['hits'] as $hit)
        {
            $row = $hit['_source'];

            if ($addId)
            {
                $row = array_merge(['id' => $hit['_id']], $row);
            }

            $results[] = $rowConverter($row);
        }

        return $results;
    }


    /**
     * Názvy klíčů, pod kterými přijdou ve výsledku hodnoty agregací, např. `min_age`.
     * @return list<string>
     */
    private static function aggregationResultKeys(FindByParams $params): array
    {
        $keys = [];

        foreach ($params->aggregation as $agg => $columns)
        {
            foreach ($columns as $column)
            {
                $keys[] = "{$agg}_{$column}";
            }
        }

        return $keys;
    }
}
