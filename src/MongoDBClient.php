<?php

namespace Hovjacky\NoSQL;

use MongoDB\BSON\Regex;
use MongoDB\Client;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Model\BSONDocument;
use \MongoDB\BSON\UTCDateTime;

class MongoDBClient extends DBWithBooleanParsing implements DBInterface
{
    /**
     * @var \MongoDB\Database
     */
    private $client;

    /**
     * MongoDBClient constructor.
     *
     * @param array $params
     *
     * @throws DBException
     */
    public function __construct($params)
    {
        $dbName = $params['db'];
        $uri = 'mongodb://' . $params['host'] . (!empty($params['port']) ? ':' . $params['port'] : '');
        $client = new Client($uri);
        $this->checkIfDbExists($client, $dbName);
        $this->client = $client->$dbName;
        $this->dbName = $dbName;
    }

    /**
     * @return \MongoDB\Database
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
     * @throws DBException
     * @throws \Exception
     */
    public function insert($table, $data)
    {
        if (!empty($data['id']))
        {
            $data['_id'] = $data['id'];
        }

        try
        {
            if (!empty($data['_id']) && $this->get($table, $data['_id']))
            {
                $this->update($table, $data['_id'], $data);
                return TRUE;
            }
            $result = $this->client->$table->insertOne($this->convertToDBDataTypes($data));
            return $result->isAcknowledged();
        }
        catch(\Exception $e)
        {
            // nemělo by nastat
            if (strpos($e->getMessage(), 'E11000 duplicate key error collection') !== FALSE)
            {
                throw new DBException(self::ERROR_INSERT);
            }
            throw $e;
        }
    }

    /**
     * @param string $table
     * @param array $data
     *
     * @return bool|mixed
     * @throws DBException
     */
    public function bulkInsert($table, $data)
    {
        if (empty($data) || !is_array($data))
        {
            throw new DBException(self::ERROR_BULK_INSERT);
        }

        $this->checkIfCollectionExists($table);

        foreach ($data as $key => $row) {
            $data[$key] = $this->convertToDBDataTypes($row);
            if (isset($row['id']))
            {
                $data[$key]['_id'] = $row['id'];
                unset($row['id']);
            }
        }

        try
        {
            $this->client->$table->insertMany($data, ['ordered' => FALSE]);
        }
        catch (BulkWriteException $e)
        {
            if (strpos($e->getMessage(), 'E11000 duplicate key error collection') !== FALSE)
            {
                $ids = [];
                foreach ($e->getWriteResult()->getWriteErrors() as $error)
                {
                    $message = $error->getMessage();
                    $startPos = strpos($message, 'dup key: { : ') + 13;
                    $endPos = strpos($message, ' }');
                    $id = trim(substr($message, $startPos, $endPos));
                    $ids[] = is_numeric($id) ? intval($id) : $id;
                }
                foreach ($data as $row)
                {
                    if (in_array($row['_id'], $ids))
                    {
                        $this->update($table, $row['_id'], $row);
                    }
                }
            }
            else
            {
                throw $e;
            }
        }
        return TRUE;
    }

    /**
     * @param string $table
     * @param int $id
     *
     * @return array|bool|BSONDocument
     * @throws DBException
     */
    public function get($table, $id)
    {
        $response = $this->client->$table->findOne(['_id' => $id]);
        if (!empty($response))
        {
            return $this->convertFromDBDataTypes($response);
        }
        $this->checkIfCollectionExists($table);
        return FALSE;
    }

    /**
     * @param string $table
     * @param int $id
     * @param array $data
     *
     * @return bool|mixed
     * @throws DBException
     */
    public function update($table, $id, $data)
    {
        $this->checkIfCollectionExists($table);
        $response = $this->client->$table->updateOne(
            ['_id' => $id],
            ['$set' => $this->convertToDBDataTypes($data)]
        );
        if ($response->getMatchedCount() === 0)
        {
            throw new DBException(str_replace('{$id}', $id, self::ERROR_UPDATE));
        }
        if ($response->getModifiedCount() === 0)
        {
            return FALSE;
        }
        return $response->isAcknowledged();
    }

    /**
     * @param string $table
     * @param int $id
     *
     * @return bool|mixed
     * @throws DBException
     */
    public function delete($table, $id)
    {
        $response = $this->client->$table->deleteOne(['_id' => $id]);
        if ($response->getDeletedCount() > 0 && $response->isAcknowledged())
        {
            return TRUE;
        }
        $this->checkIfCollectionExists($table);
        throw new DBException(str_replace('{$id}', $id, self::ERROR_DELETE));
    }

    /**
     * @param string $table
     *
     * @return bool
     * @throws DBException
     */
    public function deleteAll($table)
    {
        $this->checkIfCollectionExists($table);
        $this->client->$table->deleteMany([]);
        return TRUE;
    }

    /**
     * @param string $table
     * @param array $params
     *
     * @return array|int
     * @throws DBException
     */
    public function findBy($table, $params)
    {
        $params = $this->checkParams($params);
        $where = [];
        $paramsM = [];

        if (!empty($params['fields']) && empty($params['groupBy']) && empty($params['aggregation']))
        {
            foreach ($params['fields'] as $field)
            {
                $paramsM['projection'][$field] = 1;
            }
        }
        if (!empty($params['where']))
        {
            $placeholders = [];
            foreach ($params['where'] as $condition => $values)
            {
                if (count($params['where']) === 1)
                {
                    $where = $this->parseBooleanQuery($this->putValuesIntoQuery($condition, $values, TRUE, $placeholders));
                }
                else
                {
                    $where['$and'][] = $this->parseBooleanQuery($this->putValuesIntoQuery($condition, $values, TRUE, $placeholders));
                }
            }
            $where = $this->substitutePlaceholders($where, $placeholders);
        }
        if (!empty($params['limit']) && empty($params['groupBy']))
        {
            $paramsM['limit'] = $params['limit'];
            if (!empty($params['offset']))
            {
                $paramsM['skip'] = $params['offset'];
            }
        }
        if (!empty($params['groupBy']))
        {
            $paramsM['$group'] = [
                '_id' => [$params['groupBy'] => '$' . $params['groupBy']],
                'count' => ['$sum' => 1]
            ];
        }
        if (!empty($params['orderBy']))
        {
            foreach ($params['orderBy'] as $column)
            {
                $desc = strpos($column, ' desc');
                if ($desc !== FALSE)
                {
                    $column = substr($column, 0, $desc);
                }
                if (empty($params['groupBy']) && empty($params['aggregation']))
                {
                    $paramsM['sort'][$column] = $desc !== FALSE ? -1 : 1;
                }
                else
                {
                    $paramsM['$sort'][((!empty($params['groupBy']) && $column == $params['groupBy']) ? '_id.' . $column : $column)] = $desc !== FALSE ? -1 : 1;
                }
            }
        }
        if (!empty($params['aggregation']))
        {
            foreach ($params['aggregation'] as $agg => $columns)
            {
                if (empty($params['groupBy']))
                {
                    $paramsM['$group']['_id'] = NULL;
                }

                if (!empty($where))
                {
                    $paramsM = array_merge(['$match' => $where], $paramsM);
                }
                foreach ($columns as $column)
                {
                    $paramsM['$group']["{$agg}_{$column}"]['$' . $agg] = '$' . $column;
                }
            }
        }
        if (!empty($params['count']))
        {
                return $this->client->$table->count($where);
        }
        if (!empty($params['limit']) && !empty($params['groupBy']))
        {
            /**
             * Nejprve limit, protože pokud je za $sort má lepší optimalizaci.
             * Pokud máme offset, musíme ho ale přičíst
             */
            $paramsM['$limit'] = $params['limit'] + (!empty($params['offset']) ? $params['offset'] : 0);
            if (!empty($params['offset']))
            {
                /**
                 * N prvních řádků nevrátí
                 */
                $paramsM['$skip'] = $params['offset'];
            }
        }

        $finalResults = [];
        if (!empty($params['groupBy']) || !empty($params['aggregation']))
        {
            $paramsAgg = [];
            foreach ($paramsM as $op => $val)
            {
                $paramsAgg[][$op] = $val;
            }
            $results = $this->client->$table->aggregate($paramsAgg);
            foreach ($results as $result)
            {
                $result = (array) $result;
                if (isset($result['_id'])/* && is_array($result['_id'])*/)
                {
                    foreach ($result['_id'] as $key => $value)
                    {
                        $result[$key] = $value;
                    }
                }
                unset($result['_id']);
                $finalResults[] = $this->convertFromDBDataTypes($result);
            }
        }
        else
        {
            $results = $this->client->$table->find($where, $paramsM);
            foreach ($results as $result)
            {
                if (!empty($params['fields']) && in_array('id', $params['fields']))
                {
                    $result['id'] = $result['_id'];
                }
                unset($result['_id']);
                $finalResults[] = $this->convertFromDBDataTypes($result);
            }
        }
        if (empty($finalResults))
        {
            $this->checkIfCollectionExists($table);
        }
        return $finalResults;
    }

	public function convertToDBDataTypes($data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => $value)
            {
                if ($value instanceof \DateTime)
                {
                    $data[$key] = new UTCDateTime($value->getTimestamp() * 1000);
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
        if ($data instanceof BSONDocument)
        {
            $data = (array) $data;
        }
        if (is_array($data))
        {
            foreach ($data as $key => $value)
            {
                if ($value instanceof BSONDocument)
                {
                    $value = (array) $value;
                }
                if ($value instanceof UTCDateTime)
                {
                    $data[$key] = $value->toDateTime()->setTimezone(new \DateTimeZone('Europe/Prague'));
                }
                elseif (is_array($value))
                {
                    $data[$key] = $this->convertFromDBDataTypes($value);
                }
            }
        }
        return $data;
    }

    protected function addAndClause(&$result, $clause)
    {
        $result['$and'][] = $clause;
    }

    protected function addOrClause(&$result, $clause)
    {
        $result['$or'][] = $clause;
    }

    /**
     * Rozparsuje booleovský výraz bez závorek (jen AND a OR)
     *
     * @param $query
     * @return array
     */
    protected function parseAndOrQuery($query)
    {
        $ands = explode(' OR ', $query);
        $result = [];

        foreach ($ands as $and)
        {
            $exprs = explode(' AND ', $and);
            if (count($exprs) === 1 && count($ands) === 1)
            {
                $result = $this->parseExpression($exprs[0]);
            }
            else
            {
                $partialResult = [];
                foreach ($exprs as $expr)
                {
                    $partialResult = array_merge_recursive($partialResult, $this->parseExpression($expr));
                }
                if (count($ands) == 1)
                {
                    $result = $partialResult;
                }
                else
                {
                    $result['$or'][] = $partialResult;
                }
            }
        }
        return $result;
    }

    /**
     * Rozparsuje výraz proměnná >,<,=,>=,<=,!= hodnota
     *
     * @param $expr
     * @return mixed
     */
    protected function parseExpression($expr)
    {
        if (($pos = strpos($expr, '<=')) !== FALSE)
        {
            $val = trim(substr($expr, $pos + 2));
            $result[trim(substr($expr, 0, $pos))]['$lte'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '>=')) !== FALSE)
        {
            $val = trim(substr($expr, $pos + 2));
            $result[trim(substr($expr, 0, $pos))]['$gte'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '!=')) !== FALSE)
        {
            $val = trim(substr($expr, $pos + 2));
            $result[trim(substr($expr, 0, $pos))]['$ne'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '<')) !== FALSE)
        {
            $val = trim(substr($expr, $pos + 1));
            $result[trim(substr($expr, 0, $pos))]['$lt'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '>')) !== FALSE)
        {
            $val = trim(substr($expr, $pos + 1));
            $result[trim(substr($expr, 0, $pos))]['$gt'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '=')) !== FALSE)
        {
            $val = trim(substr($expr, $pos + 1));
            $result[trim(substr($expr, 0, $pos))] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, ' LIKE ')) !== FALSE)
        {
            $val = trim(substr($expr, $pos + 6));
            // musí začínat danou sekvencí znaků?
            if ($val[0] !== '%')
            {
                $val = '^' . $val;
            }
            else
            {
                $val = substr($val, 1);
            }
            // musí končit danou sekvencí znaků?
            if ($val[mb_strlen($val) - 1] !== '%')
            {
                $val = $val . '$';
            }
            else
            {
                $val = substr($val, 0, mb_strlen($val) - 1);
            }
            // zbytek procent nahradíme .* => +- cokoliv
            $val = str_replace('%', '.*', $val);
            $result[trim(substr($expr, 0, $pos))] = new Regex($val, "i");
            return $result;
        }
        return $expr;
    }

    /**
     * Zjistí, jestli existuje daná databáze
     *
     * @param \MongoDB\Client $client
     * @param string $dbName
     * @return bool
     * @throws DBException
     */
    private function checkIfDbExists($client, $dbName)
    {
        $dbs = $client->listDatabases();
        foreach ($dbs as $db)
        {
            if ($db->getName() == $dbName)
            {
                return TRUE;
            }
        }
        throw new DBException(str_replace('{$db}', $dbName, self::ERROR_DB_DOESNT_EXIST));
    }

    /**
     * Zjistí, jestli existuje daná kolekce ($table)
     *
     * @param string $table
     * @return bool
     * @throws DBException
     */
    private function checkIfCollectionExists($table)
    {
        $collections = $this->client->listCollections();
        foreach ($collections as $collection)
        {
            if ($collection->getName() == $table)
            {
                return TRUE;
            }
        }
        throw new DBException(
            str_replace('{$db}', $this->dbName,
                str_replace('{$table}', $table, self::ERROR_TABLE_DOESNT_EXIST)
            ));
    }

    /**
     * Nahradí placeholdery pro datum správnými hodnotami typu UTCDateTime.
     *
     * @param array $where Podmínka
     * @param array $placeholders Placeholdery a k nim správné hodnoty
     * @return mixed
     */
    private function substitutePlaceholders($where, $placeholders)
    {
        foreach ($where as $key => $value)
        {
            if (is_array($value))
            {
                $where[$key] = $this->substitutePlaceholders($value, $placeholders);
            }
            elseif (!($value instanceof Regex) && isset($placeholders[$value]))
            {
                $where[$key] = new UTCDateTime($placeholders[$value]->getTimestamp() * 1000);
            }
        }
        return $where;
    }
}