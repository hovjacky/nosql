<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;

/**
 * Zkontrolované a znormalizované parametry metody findBy().
 *
 * Kontroly i normalizace jsou stejné jako v původní metodě DB::checkAndRepairParams(),
 * která na tuto třídu teď deleguje. Navíc je z parametrů rovnou vytažen typovaný tvar,
 * takže se s nimi dál nemusí pracovat přes `empty()` a `is_array()`.
 */
final class FindByParams
{
    /** @var list<string>|null sloupce, které se mají vrátit */
    public readonly ?array $fields;

    public readonly ?int $limit;

    public readonly ?int $offset;

    public readonly bool $count;

    /** @var list<OrderByField> */
    public readonly array $orderBy;

    /** Sloupec pro seskupení, nebo `script` při použití groupByScript. */
    public readonly mixed $groupBy;

    public readonly mixed $groupByScript;

    /** @var list<OrderByField> řazení uvnitř jednoho GROUP BY bucketu */
    public readonly array $groupInternalOrderBy;

    /** @var array<string, list<string>> agregační funkce -> sloupce */
    public readonly array $aggregation;

    /** @var array<string, mixed[]|null> where podmínka -> hodnoty pro její placeholdery */
    public readonly array $where;


    /**
     * @param array<string, mixed> $normalized parametry po kontrole a normalizaci
     */
    private function __construct(private readonly array $normalized)
    {
        $fields = $normalized[DB::PARAM_FIELDS] ?? null;
        $this->fields = is_array($fields) && $fields !== [] ? array_values($fields) : null;

        $this->limit = self::toPositiveInt($normalized[DB::PARAM_LIMIT] ?? null);
        $this->offset = self::toPositiveInt($normalized[DB::PARAM_OFFSET] ?? null);
        $this->count = !empty($normalized[DB::PARAM_COUNT]);

        $this->orderBy = self::toOrderByFields($normalized[DB::PARAM_ORDER_BY] ?? null);
        $this->groupInternalOrderBy = self::toOrderByFields($normalized[DB::PARAM_GROUP_INTERNAL_ORDER_BY] ?? null);

        $this->groupBy = empty($normalized[DB::PARAM_GROUP_BY]) ? null : $normalized[DB::PARAM_GROUP_BY];
        $this->groupByScript = empty($normalized[DB::PARAM_GROUP_BY_SCRIPT])
            ? null
            : $normalized[DB::PARAM_GROUP_BY_SCRIPT];

        $this->aggregation = self::toAggregation($normalized[DB::PARAM_AGGREGATION] ?? null);
        $this->where = self::toWhere($normalized[DB::PARAM_WHERE] ?? null);
    }


    /**
     * @param array<string, mixed> $params
     * @throws DBException
     */
    public static function fromArray(array $params): self
    {
        return new self(self::normalize($params));
    }


    /**
     * Parametry v původním (znormalizovaném) tvaru včetně klíčů, kterým knihovna nerozumí.
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return $this->normalized;
    }


    /**
     * Zkontroluje parametry a sjednotí jejich tvar (skalární hodnoty obalí polem apod.).
     * @param array<string, mixed> $params
     * @return array<string, mixed>
     * @throws DBException
     */
    private static function normalize(array $params): array
    {
        if (!empty($params[DB::PARAM_FIELDS]) && !is_array($params[DB::PARAM_FIELDS]))
        {
            throw new DBException(DB::ERROR_FIELDS);
        }

        if (!empty($params[DB::PARAM_WHERE]))
        {
            if (!is_array($params[DB::PARAM_WHERE]))
            {
                throw new DBException(DB::ERROR_WHERE);
            }

            foreach ($params[DB::PARAM_WHERE] as $condition => $values)
            {
                if (is_numeric($condition))
                {
                    throw new DBException(DB::ERROR_WHERE);
                }

                if (isset($values) && !is_array($values))
                {
                    $params[DB::PARAM_WHERE][$condition] = [$values];
                }
            }
        }

        // Při získávání počtu záznamů řazení nefunguje (ani ho nepotřebujeme).
        $params = self::normalizeOrderBy($params, DB::PARAM_ORDER_BY);
        $params = self::normalizeOrderBy($params, DB::PARAM_GROUP_INTERNAL_ORDER_BY);

        if (!empty($params[DB::PARAM_AGGREGATION]))
        {
            if (!is_array($params[DB::PARAM_AGGREGATION]))
            {
                throw new DBException(DB::ERROR_AGGREGATION);
            }

            foreach ($params[DB::PARAM_AGGREGATION] as $agg => $columns)
            {
                if (!is_array($columns))
                {
                    $params[DB::PARAM_AGGREGATION][$agg] = [$columns];
                }
            }
        }

        return $params;
    }


    /**
     * @param array<string, mixed> $params
     * @return array<string, mixed>
     */
    private static function normalizeOrderBy(array $params, string $key): array
    {
        if (empty($params[$key]))
        {
            return $params;
        }

        if (!is_array($params[$key]))
        {
            $params[$key] = [$params[$key]];
        }

        if (!empty($params[DB::PARAM_COUNT]))
        {
            unset($params[$key]);
        }

        return $params;
    }


    /**
     * @return list<OrderByField>
     */
    private static function toOrderByFields(mixed $value): array
    {
        if (!is_array($value))
        {
            return [];
        }

        return array_values(array_map(
            static fn (string $column): OrderByField => OrderByField::fromString($column),
            $value,
        ));
    }


    /**
     * @return array<string, list<string>>
     */
    private static function toAggregation(mixed $value): array
    {
        if (!is_array($value))
        {
            return [];
        }

        $aggregation = [];

        foreach ($value as $agg => $columns)
        {
            $aggregation[(string) $agg] = array_values(array_map(
                static fn (mixed $column): string => (string) $column,
                is_array($columns) ? $columns : [$columns],
            ));
        }

        return $aggregation;
    }


    /**
     * @return array<string, mixed[]|null>
     */
    private static function toWhere(mixed $value): array
    {
        if (!is_array($value))
        {
            return [];
        }

        $where = [];

        foreach ($value as $condition => $values)
        {
            // normalize() sem pouští jen pole nebo null.
            $where[(string) $condition] = is_array($values) ? $values : ($values === null ? null : [$values]);
        }

        return $where;
    }


    /**
     * Limit a offset dávají smysl jen jako kladná čísla, cokoliv jiného bereme jako neuvedené.
     * Záporné číslo by Elasticsearch odmítl s chybou 400, takže se zahazuje také.
     */
    private static function toPositiveInt(mixed $value): ?int
    {
        return is_numeric($value) && (int) $value > 0 ? (int) $value : null;
    }
}
