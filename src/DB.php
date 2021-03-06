<?php

namespace Hovjacky\NoSQL;

abstract class DB implements DBInterface
{
    const ERROR_INSERT = 'Záznam není možné vložit.';
    const ERROR_BULK_INSERT = 'Prázdná nebo špatná data pro bulk insert.';
    const ERROR_BULK_INSERT_EXISTS = 'Záznam {$id} již existuje.';
    const ERROR_BULK_INSERT_ERROR = 'Chyba při hromadném zápisu do databáze.';
    const ERROR_UPDATE = 'Záznam {$id} nebyl nenalezen a tudíž ani upraven.';
    const ERROR_DELETE = 'Záznam {$id} nebyl nenalezen a tudíž ani smazán.';
    const ERROR_FIELDS = 'Parametr fields musí být pole.';
    const ERROR_WHERE = 'Parametr where musí být pole jako např. [podmínka1 => [hodnoty], podmínka2 => [hodnoty]].';
    const ERROR_AGGREGATION = 'Parametr aggregation musí být pole jako např. ["sum" => ["column1", "column2"], "avg" => "column1"].';
    const ERROR_DB_DOESNT_EXIST = 'Databáze/index {$db} neexistuje.';
    const ERROR_TABLE_DOESNT_EXIST = 'Tabulka/kolekce/typ {$table} v databázi {$db} neexistuje.';
    const ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS = 'Rozdílný počet `?` a hodnot v dotazu.';
    const ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES = 'Špatný počet závorek v dotazu.';

    const PARAM_FIELDS = 'fields';
    const PARAM_LIMIT = 'limit';
    const PARAM_OFFSET = 'offset';
    const PARAM_COUNT = 'count';
    const PARAM_ORDER_BY = 'orderBy';
    const PARAM_GROUP_BY = 'groupBy';
    const PARAM_GROUP_BY_SCRIPT = 'groupByScript';
    const PARAM_GROUP_INTERNAL_ORDER_BY = 'groupInternalOrderBy';
    const PARAM_AGGREGATION = 'aggregation';
    const PARAM_WHERE = 'where';

    const ORDER_BY_DESC_POSTFIX = ' desc';

    /**
     * @var String název databáze/indexu
     */
    protected $dbName;

    /**
     * DB constructor.
     * @param array $params
     */
    public abstract function __construct($params);

    /**
     * Vrátí klienta pro možnost vytvoření jakéhokoliv dotazu vázaného přímo na danou databázi.
     *
     * @return mixed
     */
    public abstract function getClient();

    /**
     * Vloží záznam do tabulky.
     * Pokud již existuje, tak ho upraví.
     *
     * @param string $table
     * @param array $data
     * @return mixed true nebo false podle úspěchu
     * @throws \Exception
     */
    public abstract function insert($table, $data);

    /**
     * Hromadně vloží data do tabulky.
     * Pokud již záznam existuje, tak ho upraví.
     *
     * @param string $table
     * @param array $data
     * @return mixed true nebo false podle úspěchu
     * @throws \Exception
     */
    public abstract function bulkInsert($table, $data);

    /**
     * Přečte záznam z tabulky.
     *
     * @param string $table
     * @param int $id
     * @return array|bool Nalezený záznam nebo false
     * @throws \Exception
     */
    public abstract function get($table, $id);

    /**
     * Upraví záznam v tabulce.
     *
     * @param string $table
     * @param int $id
     * @param array $data
     * @return mixed true pokud byl záznam upraven, false pokud nebyl upraven
     * @throws \Exception
     */
    public abstract function update($table, $id, $data);

    /**
     * Smaže záznam z tabulky.
     *
     * @param string $table
     * @param int $id
     * @return mixed true pokud byl záznam smazán
     * @throws \Exception
     */
    public abstract function delete($table, $id);

    /**
     * Smaže všechny záznamy z tabulky.
     *
     * @param string $table
     * @return bool
     * @throws \Exception
     */
    public abstract function deleteAll($table);

    /**
     * Vrátí záznamy odpovídající daným kritériím
     *
     * @param string $table
     * @param array $params
     * @return int|array
     * @throws \Exception
     */
    public abstract function findBy($table, $params);

    /**
     * Překonvertuje některé datové typy pro databázi
     *
     * @param $data array
     * @return array
     */
	public abstract function convertToDBDataTypes($data);

    /**
     * Překonvertuje některé datové typy pro PHP
     *
     * @param $data array
     * @return array
     */
	public abstract function convertFromDBDataTypes($data);

    /**
     * Zkontroluje parametry pro findBy, popř. vyhodí výjimku nebo je upraví
     *
     * @param array $params
     * @return mixed
     * @throws DBException
     */
    protected function checkParams($params)
    {
        if (!empty($params[self::PARAM_FIELDS]))
        {
            if (!is_array($params[self::PARAM_FIELDS]))
            {
                throw new DBException(self::ERROR_FIELDS);
            }
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