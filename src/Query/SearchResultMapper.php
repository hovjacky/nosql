<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use Closure;
use Hovjacky\NoSQL\DB;
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
     * @param array<string, mixed> $params parametry findBy()
     * @param Closure(mixed[]): mixed[] $rowConverter převod hodnot z databázových typů na PHP
     * @return array<int, array<string, mixed>>|int
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    public function map(array $response, array $params, Closure $rowConverter): array|int
    {
        if (!empty($params[DB::PARAM_GROUP_BY]))
        {
            return $this->mapBuckets($response, $params, $rowConverter);
        }

        if (is_array($params[DB::PARAM_AGGREGATION] ?? null) && !empty($params[DB::PARAM_AGGREGATION]))
        {
            return $this->mapAggregations($response, $params, $rowConverter);
        }

        return $this->mapHits($response, $params, $rowConverter);
    }


    /**
     * Výsledky seskupené přes GROUP BY, tj. buckets agregace `group_by`.
     * @param array<string, mixed> $params
     * @param Closure(mixed[]): mixed[] $rowConverter
     * @return array<int, array<string, mixed>>|int
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    private function mapBuckets(array $response, array $params, Closure $rowConverter): array|int
    {
        $buckets = $response['aggregations']['group_by']['buckets'];

        // Pokud zjišťujeme pouze počet záznamů, zajímá nás počet buckets
        if (!empty($params[DB::PARAM_COUNT]))
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

        if (!empty($params[DB::PARAM_OFFSET]) && is_numeric($params[DB::PARAM_OFFSET]))
        {
            $results = array_slice($results, (int) $params[DB::PARAM_OFFSET]);
        }

        return $results;
    }


    /**
     * @param array<string, mixed> $params
     * @return array<string, mixed>
     * @phpstan-ignore missingType.iterableValue
     */
    private function bucketToRow(array $bucket, array $params): array
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
            $row = [$params[DB::PARAM_GROUP_BY] => $bucket['key'], 'count' => $bucket['doc_count']];
        }

        foreach (self::aggregationColumns($params) as $resultKey => $column)
        {
            $row[$resultKey] = $bucket[$resultKey]['value'];
        }

        return $row;
    }


    /**
     * Agregace bez GROUP BY - výsledkem je jediný řádek s hodnotami agregací.
     * @param array<string, mixed> $params
     * @param Closure(mixed[]): mixed[] $rowConverter
     * @return array<int, array<string, mixed>>
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    private function mapAggregations(array $response, array $params, Closure $rowConverter): array
    {
        $row = [];

        foreach (self::aggregationColumns($params) as $resultKey => $column)
        {
            $row[$resultKey] = $response['aggregations'][$resultKey]['value'];
        }

        return [$rowConverter($row)];
    }


    /**
     * Běžné výsledky vyhledávání.
     * @param array<string, mixed> $params
     * @param Closure(mixed[]): mixed[] $rowConverter
     * @return array<int, array<string, mixed>>
     * @throws Throwable
     * @phpstan-ignore missingType.iterableValue
     */
    private function mapHits(array $response, array $params, Closure $rowConverter): array
    {
        // `id` není součástí `_source`, musí se doplnit z `_id`.
        $addId = is_array($params[DB::PARAM_FIELDS] ?? null)
            && !empty($params[DB::PARAM_FIELDS])
            && in_array('id', $params[DB::PARAM_FIELDS]);

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
     * Názvy sloupců s hodnotami agregací ve výsledku (`min_age`) -> název sloupce (`age`).
     * @param array<string, mixed> $params
     * @return array<string, string>
     */
    private static function aggregationColumns(array $params): array
    {
        if (!is_array($params[DB::PARAM_AGGREGATION] ?? null) || empty($params[DB::PARAM_AGGREGATION]))
        {
            return [];
        }

        $columns = [];

        foreach ($params[DB::PARAM_AGGREGATION] as $agg => $aggColumns)
        {
            if (!is_array($aggColumns))
            {
                $aggColumns = [$aggColumns];
            }

            foreach ($aggColumns as $column)
            {
                $columns["{$agg}_{$column}"] = $column;
            }
        }

        return $columns;
    }
}
