<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use Closure;
use Hovjacky\NoSQL\DB;

/**
 * Sestaví tělo Elasticsearch dotazu z parametrů metody findBy().
 */
final class SearchRequestBuilder
{
    /** Název agregace, kterou se počítají skupiny GROUP BY. */
    public const GROUP_COUNT_AGGREGATION = 'group_count';


    /**
     * Přesnost agregace `cardinality`, kterou se počítají skupiny GROUP BY.
     * Do tohoto počtu odlišných hodnot je výsledek přesný, výš je odhad. 40000 je maximum,
     * které Elasticsearch dovolí.
     */
    private const CARDINALITY_PRECISION = 40000;


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
        // Počet skupin se nedá spočítat z vrácených bucketů, těch chodí jen `size`.
        if ($params->count && $params->groupBy !== null)
        {
            return $this->buildGroupCountRequest($tableName, $params, $whereCompiler);
        }

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
     * Dotaz na počet záznamů pro endpoint `_count`.
     *
     * Ten v těle přijímá jedinou položku, `query`; `size`, `from` ani `aggs` by odmítl
     * s chybou 400, proto se dotaz nestaví z build(), ale samostatně.
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler
     * @return array<string, mixed>
     */
    public function buildCountRequest(string $tableName, FindByParams $params, Closure $whereCompiler): array
    {
        $request = [
            'index' => $tableName,
            'body' => [],
        ];

        $this->applyWhere($request, $params, $whereCompiler);

        return $request;
    }


    /**
     * Dotaz na počet skupin GROUP BY.
     *
     * Buckety by se musely spočítat všechny, jenže `terms` jich vrátí nejvýš `size`
     * a zbytek jen přičte do `sum_other_doc_count`. Přesný počet skupin proto zjišťuje
     * agregace `cardinality`, tedy obdoba `COUNT(DISTINCT sloupec)`.
     * @param Closure(string, mixed[]|null): array<string, mixed> $whereCompiler
     * @return array<string, mixed>
     */
    private function buildGroupCountRequest(string $tableName, FindByParams $params, Closure $whereCompiler): array
    {
        $request = [
            'index' => $tableName,
            'body' => [],
        ];

        $this->applyWhere($request, $params, $whereCompiler);

        // Samotné záznamy nás nezajímají, jen počet skupin.
        $request['body']['size'] = 0;
        $request['body']['aggs'][self::GROUP_COUNT_AGGREGATION]['cardinality']
            = $this->groupSource($params) + ['precision_threshold' => self::CARDINALITY_PRECISION];

        return $request;
    }


    /**
     * Podle čeho se seskupuje - sloupec, nebo skript.
     * @return array<string, mixed>
     */
    private function groupSource(FindByParams $params): array
    {
        // Je možné v groupBy uvést "script" a definovat skript v groupByScript
        return $params->groupBy !== 'script' || $params->groupByScript === null
            ? ['field' => $params->groupBy]
            : ['script' => $params->groupByScript];
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

        // Doplňuje se k případnému `size`, které nastavil applyLimit().
        foreach ($this->groupSource($params) as $key => $value)
        {
            $request['body']['aggs']['group_by']['terms'][$key] = $value;
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
