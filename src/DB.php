<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

/**
 * Class DB
 * @package Hovjacky\NoSQL
 */
abstract class DB implements DBInterface
{
    /* @var string - chybová hláška */
    public const ERROR_INSERT = 'Záznam není možné vložit.';

    /* @var string - chybová hláška */
    public const ERROR_BULK_INSERT = 'Prázdná nebo špatná data pro bulk insert.';

    /* @var string - chybová hláška */
    public const ERROR_BULK_INSERT_EXISTS = 'Záznam {$id} již existuje.';

    /* @var string - chybová hláška */
    public const ERROR_BULK_INSERT_ERROR = 'Chyba při hromadném zápisu do databáze.';

    /* @var string - chybová hláška */
    public const ERROR_UPDATE = 'Záznam {$id} nebyl nenalezen a tudíž ani upraven.';

    /* @var string - chybová hláška */
    public const ERROR_DELETE = 'Záznam {$id} nebyl nenalezen a tudíž ani smazán.';

    /* @var string - chybová hláška */
    public const ERROR_FIELDS = 'Parametr fields musí být pole.';

    /* @var string - chybová hláška */
    public const ERROR_WHERE = 'Parametr where musí být pole jako např. [podmínka1 => [hodnoty], podmínka2 => [hodnoty]].';

    /* @var string - chybová hláška */
    public const ERROR_AGGREGATION = 'Parametr aggregation musí být pole jako např. ["sum" => ["column1", "column2"], "avg" => "column1"].';

    /* @var string - chybová hláška */
    public const ERROR_DB_DOESNT_EXIST = 'Databáze/index neexistuje.';

    /* @var string - chybová hláška */
    public const ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS = 'Rozdílný počet `?` a hodnot v dotazu.';

    /* @var string - chybová hláška */
    public const ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES = 'Špatný počet závorek v dotazu.';

    
    /* @var string - parametr */
    public const PARAM_FIELDS = 'fields';

    /* @var string - parametr */
    public const PARAM_LIMIT = 'limit';

    /* @var string - parametr */
    public const PARAM_OFFSET = 'offset';

    /* @var string - parametr */
    public const PARAM_COUNT = 'count';

    /* @var string - parametr */
    public const PARAM_ORDER_BY = 'orderBy';

    /* @var string - parametr */
    public const PARAM_GROUP_BY = 'groupBy';

    /* @var string - parametr */
    public const PARAM_GROUP_BY_SCRIPT = 'groupByScript';

    /* @var string - parametr */
    public const PARAM_GROUP_INTERNAL_ORDER_BY = 'groupInternalOrderBy';

    /* @var string - parametr */
    public const PARAM_AGGREGATION = 'aggregation';

    /* @var string - parametr */
    public const PARAM_WHERE = 'where';


    /* @var string - typ seřazení */
    public const ORDER_BY_DESC_POSTFIX = ' desc';


    /**
     * DB constructor.
     * @param array<string, scalar> $params Parametry proměnných: Název -> hodnota
     */
    abstract public function __construct(array $params);


    /**
     * Zkontroluje parametry pro findBy, popř. vyhodí výjimku nebo je upraví.
     * @param array<string, mixed> $params Sloupec -> hodnota
     * @return array<string, mixed> Sloupec -> upravená hodnota
     * @throws DBException
     */
    protected function checkAndRepairParams(array $params): array
    {
        if (!empty($params[self::PARAM_FIELDS]) && !is_array($params[self::PARAM_FIELDS]))
        {
            throw new DBException(self::ERROR_FIELDS);
        }

        if (!empty($params[self::PARAM_WHERE]))
        {
            if (!is_array($params[self::PARAM_WHERE]))
            {
                throw new DBException(self::ERROR_WHERE);
            }
            foreach ($params[self::PARAM_WHERE] as $condition => $values)
            {
                if (is_numeric($condition))
                {
                    throw new DBException(self::ERROR_WHERE);
                }
                if (isset($values) && !is_array($values))
                {
                    $params[self::PARAM_WHERE][$condition] = [$values];
                }
            }
        }

        if (!empty($params[self::PARAM_ORDER_BY]))
        {
            if (!is_array($params[self::PARAM_ORDER_BY]))
            {
                $params[self::PARAM_ORDER_BY] = [$params[self::PARAM_ORDER_BY]];
            }

            // Při získávání počtu záznamů nefunguje orderBy (ani jej nepotřebujeme)
            if (!empty($params[self::PARAM_COUNT]))
            {
                unset($params[self::PARAM_ORDER_BY]);
            }
        }

        if (!empty($params[self::PARAM_GROUP_INTERNAL_ORDER_BY]))
        {
            if (!is_array($params[self::PARAM_GROUP_INTERNAL_ORDER_BY]))
            {
                $params[self::PARAM_GROUP_INTERNAL_ORDER_BY] = [$params[self::PARAM_GROUP_INTERNAL_ORDER_BY]];
            }
            // Při získávání počtu záznamů nepotřebujeme groupInternalOrderBy
            if (!empty($params[self::PARAM_COUNT]))
            {
                unset($params[self::PARAM_GROUP_INTERNAL_ORDER_BY]);
            }
        }

        if (!empty($params[self::PARAM_AGGREGATION]))
        {
            if (!is_array($params[self::PARAM_AGGREGATION]))
            {
                throw new DBException(self::ERROR_AGGREGATION);
            }
            foreach ($params[self::PARAM_AGGREGATION] as $agg => $columns)
            {
                if (!is_array($columns))
                {
                    $params[self::PARAM_AGGREGATION][$agg] = [$columns];
                }
            }
        }

        return $params;
    }
}
