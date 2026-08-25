<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use Closure;
use Hovjacky\NoSQL\DB;

/**
 * Sestaví tělo Elasticsearch dotazu z parametrů metody findBy().
 *
 * Parametry se očekávají už zkontrolované a znormalizované metodou DB::checkAndRepairParams().
 */
final class SearchRequestBuilder
{
    /**
     * @param int $defaultLimit limit použitý, pokud volající žádný neuvede
     */
    public function __construct(private readonly int $defaultLimit)
    {
    }


    /**
     * @param array<string, mixed> $params parametry findBy()
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler přeloží jednu where podmínku na Elasticsearch query
     * @return array<string, mixed>
     */
    public function build(string $tableName, array $params, Closure $whereCompiler): array
    {
        $request = [
            'index' => $tableName,
            'body' => [],
        ];

        $this->applyFields($request, $params);
        $this->applyWhere($request, $params, $whereCompiler);
        $this->applyLimit($request, $params);
        $this->applySort($request, $params);
        $this->applyGroupBy($request, $params);
        $this->applyAggregation($request, $params);

        return $request;
    }


    /**
     * Dotaz na počet záznamů. Endpoint `_count` parametr `size` nepodporuje.
     * @param array<string, mixed> $params
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler
     * @return array<string, mixed>
     */
    public function buildCountRequest(string $tableName, array $params, Closure $whereCompiler): array
    {
        $request = $this->build($tableName, $params, $whereCompiler);

        unset($request['body']['size']);

        return $request;
    }


    /**
     * @param array<string, mixed> $request
     * @param array<string, mixed> $params
     */
    private function applyFields(array &$request, array $params): void
    {
        if (is_array($params[DB::PARAM_FIELDS] ?? null) && !empty($params[DB::PARAM_FIELDS]))
        {
            $request['_source_includes'] = implode(',', $params[DB::PARAM_FIELDS]);
        }
    }


    /**
     * @param array<string, mixed> $request
     * @param array<string, mixed> $params
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler
     */
    private function applyWhere(array &$request, array $params, Closure $whereCompiler): void
    {
        if (!is_array($params[DB::PARAM_WHERE] ?? null) || empty($params[DB::PARAM_WHERE]))
        {
            return;
        }

        $conditions = [];

        foreach ($params[DB::PARAM_WHERE] as $condition => $values)
        {
            // checkAndRepairParams() sem pouští jen pole nebo null, ostatní obalíme stejně jako ono.
            if ($values !== null && !is_array($values))
            {
                $values = [$values];
            }

            $parsedBooleanQuery = $whereCompiler((string) $condition, $values);

            // Jediná podmínka se do query dá rovnou, není důvod ji balit do `bool.filter`.
            if (count($params[DB::PARAM_WHERE]) === 1)
            {
                $conditions = $parsedBooleanQuery;

                break;
            }

            $conditions['bool']['filter'][] = $parsedBooleanQuery;
        }

        $request['body']['query'] = $conditions;
    }


    /**
     * @param array<string, mixed> $request
     * @param array<string, mixed> $params
     */
    private function applyLimit(array &$request, array $params): void
    {
        if (!empty($params[DB::PARAM_LIMIT]))
        {
            if (empty($params[DB::PARAM_GROUP_BY]))
            {
                $request['body']['size'] = $params[DB::PARAM_LIMIT];

                if (!empty($params[DB::PARAM_OFFSET]))
                {
                    $request['body']['from'] = $params[DB::PARAM_OFFSET];
                }
            }
            else
            {
                // U GROUP BY se offset ořezává až nad výsledky, proto musí bucketů přijít i na offset.
                $limit = $params[DB::PARAM_LIMIT];

                if (!empty($params[DB::PARAM_OFFSET]))
                {
                    $limit += $params[DB::PARAM_OFFSET];
                }

                $request['body']['aggs']['group_by']['terms']['size'] = $limit;
            }
        }
        elseif (!empty($params[DB::PARAM_GROUP_BY]))
        {
            // Aggregation - nechceme normální výsledky
            $request['body']['size'] = 0;
            $request['body']['aggs']['group_by']['terms']['size'] = $this->defaultLimit;
        }
        elseif (empty($params[DB::PARAM_COUNT]))
        {
            $request['body']['size'] = $this->defaultLimit;
        }
    }


    /**
     * @param array<string, mixed> $request
     * @param array<string, mixed> $params
     */
    private function applySort(array &$request, array $params): void
    {
        if (
            empty($params[DB::PARAM_ORDER_BY])
            || !is_array($params[DB::PARAM_ORDER_BY])
            || !empty($params[DB::PARAM_GROUP_BY])
            || !empty($params[DB::PARAM_AGGREGATION])
        )
        {
            return;
        }

        foreach ($params[DB::PARAM_ORDER_BY] as $column)
        {
            [$column, $desc] = self::splitDescSuffix($column);

            $request['body']['sort'][] = [$column => ($desc ? 'desc' : 'asc')];
        }

        $request['body']['sort'][] = '_score';
    }


    /**
     * @param array<string, mixed> $request
     * @param array<string, mixed> $params
     */
    private function applyGroupBy(array &$request, array $params): void
    {
        if (empty($params[DB::PARAM_GROUP_BY]))
        {
            return;
        }

        // Je možné v groupBy uvést "script" a definovat skript v groupByScript
        if ($params[DB::PARAM_GROUP_BY] !== 'script' || empty($params[DB::PARAM_GROUP_BY_SCRIPT]))
        {
            $request['body']['aggs']['group_by']['terms']['field'] = $params[DB::PARAM_GROUP_BY];
        }
        else
        {
            $request['body']['aggs']['group_by']['terms']['script'] = $params[DB::PARAM_GROUP_BY_SCRIPT];
        }

        if (!is_array($params[DB::PARAM_ORDER_BY] ?? null) || empty($params[DB::PARAM_ORDER_BY]))
        {
            return;
        }

        $request['body']['aggs']['group_by']['aggs']['results']['top_hits']['size'] = 1;

        if (!empty($params[DB::PARAM_FIELDS]))
        {
            // Chceme vrátit jen požadovaná pole
            $request['body']['aggs']['group_by']['aggs']['results']['top_hits']['_source']['includes'] = $params[DB::PARAM_FIELDS];
        }

        foreach ($params[DB::PARAM_ORDER_BY] as $column)
        {
            [$column, $desc] = self::splitDescSuffix($column);

            $request['body']['aggs']['group_by']['terms']['order'][] = [
                self::bucketOrderKey($column, $params[DB::PARAM_GROUP_BY]) => ($desc ? 'desc' : 'asc'),
            ];

            // Musí se přidat agregace podle sloupce, podle kterého chceme řadit
            if ($column !== $params[DB::PARAM_GROUP_BY] && $column !== DB::PARAM_COUNT)
            {
                $request['body']['aggs']['group_by']['aggs'][$column][$desc ? 'max' : 'min']['field'] = $column;
            }
        }

        $this->applyGroupInternalOrderBy($request, $params);
    }


    /**
     * Vnitřní řazení v GROUP BY buckets (jaký záznam ze skupiny chceme).
     * @param array<string, mixed> $request
     * @param array<string, mixed> $params
     */
    private function applyGroupInternalOrderBy(array &$request, array $params): void
    {
        if (
            !is_array($params[DB::PARAM_GROUP_INTERNAL_ORDER_BY] ?? null)
            || empty($params[DB::PARAM_GROUP_INTERNAL_ORDER_BY])
        )
        {
            return;
        }

        foreach ($params[DB::PARAM_GROUP_INTERNAL_ORDER_BY] as $column)
        {
            [$column, $desc] = self::splitDescSuffix($column);

            $request['body']['aggs']['group_by']['aggs']['results']['top_hits']['sort'][] = [
                $column => ['order' => $desc ? 'desc' : 'asc'],
            ];
        }
    }


    /**
     * @param array<string, mixed> $request
     * @param array<string, mixed> $params
     */
    private function applyAggregation(array &$request, array $params): void
    {
        if (!is_array($params[DB::PARAM_AGGREGATION] ?? null) || empty($params[DB::PARAM_AGGREGATION]))
        {
            return;
        }

        foreach ($params[DB::PARAM_AGGREGATION] as $agg => $columns)
        {
            if (!is_array($columns))
            {
                $columns = [$columns];
            }

            foreach ($columns as $column)
            {
                // Bez GROUP BY jde agregace do kořene, s GROUP BY dovnitř bucketu.
                if (empty($params[DB::PARAM_GROUP_BY]))
                {
                    $request['body']['aggs']["{$agg}_{$column}"][$agg] = ['field' => $column];
                }
                else
                {
                    $request['body']['aggs']['group_by']['aggs']["{$agg}_{$column}"][$agg] = ['field' => $column];
                }
            }
        }
    }


    /**
     * Řazení bucketů podle sloupce GROUP BY je `_key`, podle počtu záznamů `_count`.
     */
    private static function bucketOrderKey(string $column, mixed $groupBy): string
    {
        if ($column === $groupBy)
        {
            return '_key';
        }

        return $column === DB::PARAM_COUNT ? '_count' : $column;
    }


    /**
     * Oddělí od názvu sloupce příponu značící sestupné řazení.
     * @return array{string, bool}
     */
    private static function splitDescSuffix(string $column): array
    {
        $desc = strpos($column, DB::ORDER_BY_DESC_POSTFIX);

        return $desc !== false ? [substr($column, 0, $desc), true] : [$column, false];
    }
}
