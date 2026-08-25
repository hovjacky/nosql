<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use Hovjacky\NoSQL\Logging\TracyLogger;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class DB implements DBInterface, LoggerAwareInterface
{
    // Chybové hlášky. Hlášky s `{$id}` se plní přes pojmenované konstruktory DBException.
    public const ERROR_INSERT = 'Záznam není možné vložit.';
    public const ERROR_BULK_INSERT = 'Prázdná nebo špatná data pro bulk insert.';
    public const ERROR_BULK_INSERT_EXISTS = 'Záznam {$id} již existuje.';
    public const ERROR_BULK_INSERT_ERROR = 'Chyba při hromadném zápisu do databáze.';
    public const ERROR_UPDATE = 'Záznam {$id} nebyl nenalezen a tudíž ani upraven.';
    public const ERROR_DELETE = 'Záznam {$id} nebyl nenalezen a tudíž ani smazán.';
    public const ERROR_FIELDS = 'Parametr fields musí být pole.';
    public const ERROR_WHERE = 'Parametr where musí být pole jako např. [podmínka1 => [hodnoty], podmínka2 => [hodnoty]].';
    public const ERROR_AGGREGATION = 'Parametr aggregation musí být pole jako např. ["sum" => ["column1", "column2"], "avg" => "column1"].';
    public const ERROR_DB_DOESNT_EXIST = 'Databáze/index neexistuje.';
    public const ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS = 'Rozdílný počet `?` a hodnot v dotazu.';
    public const ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES = 'Špatný počet závorek v dotazu.';

    // Názvy parametrů metody findBy().
    public const PARAM_FIELDS = 'fields';
    public const PARAM_LIMIT = 'limit';
    public const PARAM_OFFSET = 'offset';
    public const PARAM_COUNT = 'count';
    public const PARAM_ORDER_BY = 'orderBy';
    public const PARAM_GROUP_BY = 'groupBy';
    public const PARAM_GROUP_BY_SCRIPT = 'groupByScript';
    public const PARAM_GROUP_INTERNAL_ORDER_BY = 'groupInternalOrderBy';
    public const PARAM_AGGREGATION = 'aggregation';
    public const PARAM_WHERE = 'where';

    /** Přípona názvu sloupce v orderBy značící sestupné řazení. */
    public const ORDER_BY_DESC_POSTFIX = ' desc';


    private ?LoggerInterface $logger = null;


    /**
     * DB constructor.
     * @param array<string, scalar> $params Parametry proměnných: Název -> hodnota
     */
    abstract public function __construct(array $params);


    /**
     * Nastaví logger, do kterého knihovna hlásí chyby.
     * Bez zavolání se použije Tracy (je-li k dispozici), jinak se nikam neloguje.
     */
    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }


    /**
     * Logger pro hlášení chyb. Vytváří se líně, protože potomci nevolají konstruktor předka.
     */
    protected function getLogger(): LoggerInterface
    {
        return $this->logger ??= (TracyLogger::isAvailable() ? new TracyLogger() : new NullLogger());
    }


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
