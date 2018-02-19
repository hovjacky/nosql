<?php

namespace Hovjacky\NoSQL;

use Cassandra;

class CassandraClient extends DB implements DBInterface
{
    const ERROR_OR_IN_WHERE = 'Cassandra neumožňuje použití OR v dotazu.';
    const ERROR_UNDEFINED_ORDER = 'Pole {$orderBy}, podle kterého chcete řadit, nebylo ve výsledcích nalezeno.';

    /**
     * @var Cassandra\Session
     */
    private $client;

    /**
     * CassandraClient constructor.
     *
     * @param $params
     *
     * @throws DBException
     * @throws \Exception
     */
    public function __construct($params)
    {
        $dbName = $params['db'];
        $this->dbName = $dbName;
        try
        {
            /**
             * @var $builder Cassandra\Cluster\Builder
             */
            $builder = Cassandra::cluster();
            $port = 9042;
            if (!empty($params['port']))
            {
                $port = $params['port'];
            }
            /**
             * @var $cluster Cassandra\Cluster
             */
            $cluster = $builder->withContactPoints($params['host'])->withPort($port)->withDefaultTimeout(60)->withRequestTimeout(60)->build();
            $this->client = $cluster->connect($dbName);
        }
        catch (\Exception $e)
        {
            $this->handleException($e, '');
        }
    }

    public function __destruct()
    {
        if (!empty($this->client))
        {
            $this->client->closeAsync();
        }
    }

    /**
     * @return Cassandra\Session
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param string $table
     * @param array $data
     *
     * @return bool|mixed
     * @throws Cassandra\Exception
     * @throws DBException
     * @throws \Exception
     */
    public function insert($table, $data)
    {
        try
        {
            $statement = $this->prepareInsert($table, $data);
            $this->client->execute($statement, ['arguments' => $data]);
            /**
             * Udržujeme počet záznamů ve vedlejší tabulce pro rychlé získání
             */
            if (strpos($table, '_count') === FALSE)
            {
                $row = $this->get($table . '_count', 1);
                if (!empty($row))
                {
                    $this->update($table . '_count', 1, ['count' => $row['count'] + 1]);
                }
                else
                {
                    $row['id'] = 1;
                    $row['count'] = 1;
                    $this->insert($table . '_count', $row);
                }
            }
            return TRUE;
        }
        catch(\Exception $e)
        {
            $this->handleException($e, $table);
        }
        return FALSE;
    }

    /**
     * @param string $table
     * @param array $data
     *
     * @return bool|mixed
     * @throws Cassandra\Exception
     * @throws DBException
     * @throws \Exception
     */
    public function bulkInsert($table, $data)
    {
        if (empty($data) || !is_array($data))
        {
            throw new DBException(self::ERROR_BULK_INSERT);
        }

        try
        {
            $statement = $this->prepareInsert($table, $data[0]);
            $batch = new Cassandra\BatchStatement(Cassandra::BATCH_UNLOGGED);

            foreach ($data as $row)
            {
                $d = $this->convertToDBDataTypes($row);
                $batch->add($statement, $d);
            }

            $this->client->execute($batch);
            $row = $this->get($table . '_count', 1);
            if (!empty($row))
            {
                $this->update($table . '_count', 1, ['count' => $row['count'] + count($data)]);
            }
            else
            {
                $row['id'] = 1;
                $row['count'] = count($data);
                $this->insert($table . '_count', $row);
            }
            return TRUE;
        }
        catch(\Exception $e)
        {
            $this->handleException($e, $table);
        }
        return FALSE;
    }

    /**
     * @param string $table
     * @param int $id
     *
     * @return array|bool
     * @throws Cassandra\Exception
     * @throws DBException
     * @throws \Exception
     */
    public function get($table, $id)
    {
        try
        {
            $statement = $this->client->prepare("SELECT * FROM {$table} WHERE id = :id");
            $result = $this->client->execute($statement, ['arguments' => ['id' => $id]]);
            if ($result->count() > 0)
            {
                return $this->convertFromDBDataTypes($result->first());
            }
        }
        catch (\Exception $e)
        {
            $this->handleException($e, $table);
        }
        return FALSE;
    }

    /**
     * @param string $table
     * @param int $id
     * @param array $data
     *
     * @return bool|mixed
     * @throws Cassandra\Exception
     * @throws DBException
     * @throws \Exception
     */
    public function update($table, $id, $data)
    {
        $id = (is_numeric($id) ? (int) $id : $id);
        $row = $this->get($table, $id);
        if (empty($row))
        {
            throw new DBException(str_replace('{$id}', $id, self::ERROR_UPDATE));
        }
        $set = [];
        $values = [];
        foreach ($data as $key => $value)
        {
            if ($row[$key] != $value)
            {
                $set[] = "{$key} = ?";
                $values[$key] = $value;
            }
        }
        if (empty($set))
        {
            return FALSE;
        }
        $statement = $this->client->prepare("UPDATE {$table} SET " . implode($set, ", ") . " WHERE id = :id");
        $this->client->execute($statement, ['arguments' => array_merge($values, ['id' => $id])]);
        return TRUE;
    }

    /**
     * @param string $table
     * @param int $id
     *
     * @return bool|mixed
     * @throws Cassandra\Exception
     * @throws DBException
     * @throws \Exception
     */
    public function delete($table, $id)
    {
        $row = $this->get($table, $id);
        if (empty($row))
        {
            throw new DBException(str_replace('{$id}', $id, self::ERROR_DELETE));
        }
        $statement = $this->client->prepare("DELETE FROM {$table} WHERE id = :id");
        $this->client->execute($statement, ['arguments' => ['id' => $id]]);
        $personCountRow = $this->get($table . '_count', 1);
        $this->update($table . '_count', 1, ['count' => $personCountRow['count'] - 1]);
        return TRUE;
    }

    /**
     * @param string $table
     *
     * @return bool
     * @throws Cassandra\Exception
     * @throws DBException
     * @throws \Exception
     */
    public function deleteAll($table)
    {
        try {
            $statement = $this->client->prepare("TRUNCATE TABLE {$table}");
            $this->client->execute($statement);
            $row = $this->get($table . '_count', 1);
            if (!empty($row))
            {
                $this->update($table . '_count', 1, ['count' => 0]);
            }
            else
            {
                $row['id'] = 1;
                $row['count'] = 0;
                $this->insert($table . '_count', $row);
            }
        }
        catch (\Exception $e)
        {
            $this->handleException($e, $table);
        }
        return TRUE;
    }

    /**
     * @param string $table
     * @param array $params
     *
     * @return array|int
     * @throws Cassandra\Exception
     * @throws DBException
     * @throws \Exception
     */
    public function findBy($table, $params)
    {
        $params = $this->checkParams($params);
		
        if (count($params) === 1 && isset($params['count']))
        {
            $row = $this->get($table . '_count', 1);
            if (!empty($row))
            {
                return $row['count'];
            }
            return 0;
        }

        if (!empty($params['aggregation']))
        {
            foreach ($params['aggregation'] as $agg => $columns)
            {
                foreach ($columns as $column)
                {
                    $params['fields'][] = strtoupper($agg) . "({$column}) AS {$agg}_{$column}";
                }
            }
        }
        if (!empty($params['count']))
        {
            $params['fields'] = [];
            $params['fields'][] = "COUNT({$params['count']}) AS count";
        }
        if (empty($params['fields']))
        {
            $params['fields'][] = '*';
        }

        $values = [];
        $query = "SELECT " . implode(', ', $params['fields']) . " FROM {$table}";
        if (!empty($params['where']))
        {
            $where = [];
            foreach ($params['where'] as $cond => $val)
            {
                if (strpos($cond, ' OR ') !== FALSE)
                {
                    throw new DBException(self::ERROR_OR_IN_WHERE);
                }
                $where[] = strpos($cond, '(') ? $cond : str_replace(' AND ', ') AND (', $cond);
                $values = array_merge($values, $val);
            }
            $query .= " WHERE (" . implode(') AND (', $where) . ")";
        }
        if (!empty($params['groupBy']))
        {
            $query .= " GROUP BY {$params['groupBy']}";
        }
        if (!empty($params['limit']) && empty($params['orderBy']))
        {
            /**
             * Pokud chceme i offset vrátíme offset + length a nakonec to o offset ořežeme
             */
            $limit = !empty($params['offset']) ? $params['offset'] + $params['limit'] : $params['limit'];
            $query .= " LIMIT {$limit}";
        }
        if (!empty($params['where']))
        {
            $query .= " ALLOW FILTERING";
        }
        try
        {
            $statement = $this->client->prepare($query);
            $values = $this->convertToDBDataTypes($values);
            $rows = $this->client->execute($statement, ['arguments' => $values]);
        }
        catch (\Exception $e)
        {
            $this->handleException($e, $table);
        }

        $result = [];
        foreach ($rows as $row)
        {
            $result[] = $this->convertFromDBDataTypes($row);
        }

        if (!empty($params['count']))
        {
            return $result[0]['count']->toInt();
        }
        if (!empty($params['orderBy']))
        {
            $order = $params['orderBy'];
            /**
             * Seřaď výsledky
             */
            usort($result, function ($item1, $item2) use ($order)
            {
                return $this->orderBy($item1, $item2, $order);
            });
            if (!empty($params['limit']))
            {
                return array_slice($result, !empty($params['offset']) ? $params['offset'] : 0, $params['limit']);
            }
        }
        /**
         * Pokud byl zadán offset, musíme výsledek o něj zkrátit
         */
        if (!empty($params['offset']))
        {
            return array_slice($result, $params['offset']);
        }
        return $result;
    }

    public function convertToDBDataTypes($data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => $value)
            {
                if ($value instanceof \DateTime)
                {
                    $data[$key] = new Cassandra\Timestamp($value->getTimestamp(), 0);
                }
                elseif (is_double($value))
                {
                    $data[$key] = new Cassandra\Float($value);
                }
                elseif (is_array($value))
                {
                    $data[$key] = $this->convertToDBDataTypes($value);
                }
            }
        }
        return $data;
    }

	public function convertFromDBDataTypes($data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => $value)
            {
                if ($value instanceof Cassandra\Timestamp)
                {
                    $data[$key] = $value->toDateTime()->setTimezone(new \DateTimeZone('Europe/Prague'));
                }
                elseif ($value instanceof Cassandra\Float)
                {
                    $data[$key] = $value->value();
                }
                elseif (is_array($value))
                {
                    $data[$key] = $this->convertFromDBDataTypes($value);
                }
            }
        }
        return $data;
    }

    /**
     * Připraví insert dotaz
     *
     * @param $table string
     * @param $data array Podle klíčů v $data vezme názvy sloupců
     * @return Cassandra\PreparedStatement
     */
    private function prepareInsert($table, $data)
    {
        $fields = [];
        $questionmarks = [];
        foreach ($data as $key => $value)
        {
            $fields[] = '"' . $key . '"';
            $questionmarks[] = "?";
        }

        return $this->client->prepare("INSERT INTO {$table} (" . implode($fields, ", ") . ") " .
            "VALUES (" . implode($questionmarks, ", ") . ")");
    }

    /**
     * Seřadí položky podle indexů v poli $order, pokud se rovnají při prvním indexu, seřadí je podle druhého, atd.
     *
     * @param $item1 array První položka
     * @param $item2 array Druhá položka
     * @param $order array
     * @return int -1 pokud je $item2 > $item1, 0 pokud jsou si rovny, 1 pokud $item1 > $item2
     * @throws DBException
     */
    private function orderBy($item1, $item2, $order)
    {
        $desc = ($pos = strpos($order[0], ' desc')) !== FALSE ? TRUE : FALSE;
        if ($desc)
        {
            $order[0] = trim(substr($order[0], 0, $pos));
        }
        if (!isset($item1[$order[0]]) || !isset($item2[$order[0]]))
        {
            throw new DBException(str_replace('{$orderBy}', $order[0], self::ERROR_UNDEFINED_ORDER));
        }
        if ($item1[$order[0]] == $item2[$order[0]] && count($order) > 1)
        {
            array_shift($order);
            return $this->orderBy($item1, $item2, $order);
        }
        if (!$desc)
        {
            return $item1[$order[0]] <=> $item2[$order[0]];
        }
        else
        {
            return $item2[$order[0]] <=> $item1[$order[0]];
        }
    }

    /**
     * Odchycení společných výjimek
     *
     * @param $e \Exception
     * @param $table
     * @throws DBException
     * @throws \Exception
     */
    private function handleException($e, $table)
    {
        $code = $e->getCode();
        if (!is_numeric($e->getCode()))
        {
            $code = NULL;
        }

        if (strpos($e->getMessage(), "Keyspace '{$this->dbName}' does not exist") !== FALSE)
        {
            throw new DBException(str_replace('{$db}', $this->dbName, self::ERROR_DB_DOESNT_EXIST), $code);
        }
        else if (strpos($e->getMessage(), 'unconfigured table') !== FALSE)
        {
            throw new DBException(
                str_replace('{$db}', $this->dbName,
                    str_replace('{$table}', $table, self::ERROR_TABLE_DOESNT_EXIST)
                ), $code);
        }
        throw $e;
    }
}