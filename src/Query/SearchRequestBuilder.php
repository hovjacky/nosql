<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use Closure;
use Hovjacky\NoSQL\DB;

/**
 * Sestaví tělo Elasticsearch dotazu z parametrů metody findBy().
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
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler přeloží jednu where podmínku na Elasticsearch query
     * @return array<string, mixed>
     */
    public function build(string $tableName, FindByParams $params, Closure $whereCompiler): array
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
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler
     * @return array<string, mixed>
     */
    public function buildCountRequest(string $tableName, FindByParams $params, Closure $whereCompiler): array
    {
        $request = $this->build($tableName, $params, $whereCompiler);

        unset($request['body']['size']);

        return $request;
    }


    /**
     * @param array<string, mixed> $request
     */
    private function applyFields(array &$request, FindByParams $params): void
    {
        if ($params->fields !== null)
        {
            $request['_source_includes'] = implode(',', $params->fields);
        }
    }


    /**
     * @param array<string, mixed> $request
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler
     */
    private function applyWhere(array &$request, FindByParams $params, Closure $whereCompiler): void
    {
        if ($params->where === [])
        {
            return;
        }

        $conditions = [];

        foreach ($params->where as $condition => $values)
        {
            $parsedBooleanQuery = $whereCompiler($condition, $values);

            // Jediná podmínka se do query dá rovnou, není důvod ji balit do `bool.filter`.
            if (count($params->where) === 1)
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
     */
    private function applyLimit(array &$request, FindByParams $params): void
    {
        if ($params->limit !== null)
        {
            if ($params->groupBy === null)
            {
                $request['body']['size'] = $params->limit;

                if ($params->offset !== null)
                {
                    $request['body']['from'] = $params->offset;
                }
            }
            else
            {
                // U GROUP BY se offset ořezává až nad výsledky, proto musí bucketů přijít i na offset.
                $request['body']['aggs']['group_by']['terms']['size'] = $params->limit + ($params->offset ?? 0);
            }
        }
        elseif ($params->groupBy !== null)
        {
            // Aggregation - nechceme normální výsledky
            $request['body']['size'] = 0;
            $request['body']['aggs']['group_by']['terms']['size'] = $this->defaultLimit;
        }
        elseif (!$params->count)
        {
            $request['body']['size'] = $this->defaultLimit;
        }
    }


    /**
     * @param array<string, mixed> $request
     */
    private function applySort(array &$request, FindByParams $params): void
    {
        if ($params->orderBy === [] || $params->groupBy !== null || $params->aggregation !== [])
        {
            return;
        }

        foreach ($params->orderBy as $field)
        {
            $request['body']['sort'][] = [$field->column => $field->direction()];
        }

        $request['body']['sort'][] = '_score';
    }


    /**
     * @param array<string, mixed> $request
     */
    private function applyGroupBy(array &$request, FindByParams $params): void
    {
        if ($params->groupBy === null)
        {
            return;
        }

        // Je možné v groupBy uvést "script" a definovat skript v groupByScript
        if ($params->groupBy !== 'script' || $params->groupByScript === null)
        {
            $request['body']['aggs']['group_by']['terms']['field'] = $params->groupBy;
        }
        else
        {
            $request['body']['aggs']['group_by']['terms']['script'] = $params->groupByScript;
        }

        if ($params->orderBy === [])
        {
            return;
        }

        $request['body']['aggs']['group_by']['aggs']['results']['top_hits']['size'] = 1;

        if ($params->fields !== null)
        {
            // Chceme vrátit jen požadovaná pole
            $request['body']['aggs']['group_by']['aggs']['results']['top_hits']['_source']['includes'] = $params->fields;
        }

        foreach ($params->orderBy as $field)
        {
            $request['body']['aggs']['group_by']['terms']['order'][] = [
                self::bucketOrderKey($field->column, $params->groupBy) => $field->direction(),
            ];

            // Musí se přidat agregace podle sloupce, podle kterého chceme řadit
            if ($field->column !== $params->groupBy && $field->column !== DB::PARAM_COUNT)
            {
                $request['body']['aggs']['group_by']['aggs'][$field->column][$field->descending ? 'max' : 'min']['field'] = $field->column;
            }
        }

        $this->applyGroupInternalOrderBy($request, $params);
    }


    /**
     * Vnitřní řazení v GROUP BY buckets (jaký záznam ze skupiny chceme).
     * @param array<string, mixed> $request
     */
    private function applyGroupInternalOrderBy(array &$request, FindByParams $params): void
    {
        foreach ($params->groupInternalOrderBy as $field)
        {
            $request['body']['aggs']['group_by']['aggs']['results']['top_hits']['sort'][] = [
                $field->column => ['order' => $field->direction()],
            ];
        }
    }


    /**
     * @param array<string, mixed> $request
     */
    private function applyAggregation(array &$request, FindByParams $params): void
    {
        foreach ($params->aggregation as $agg => $columns)
        {
            foreach ($columns as $column)
            {
                // Bez GROUP BY jde agregace do kořene, s GROUP BY dovnitř bucketu.
                if ($params->groupBy === null)
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
}
