<?php

namespace Hovjacky\NoSQL;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Tracy\Debugger;

class ElasticsearchClient extends DBWithBooleanParsing implements DBInterface
{
    /**
     * @var Client
     */
    private $client;

    const DEFAULT_LIMIT = 100;
    // Tímto se to bude chovat jako SQL, vrátí jeden záznam pro jednu GROUP BY hodnotu, ale dalo by se nastavit i jinak
    const DEFAULT_GROUP_LIMIT = 1;

    public function __construct($params)
    {
        $connectionParams[] = $params['host'] . (!empty($params['port']) ? ':' . $params['port'] : '');
        $this->client = ClientBuilder::create()->setHosts($connectionParams)->build();
        $this->dbName = $params['index'];
    }

    /**
     * @return \Elasticsearch\Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param string $table
     * @param array $data
     *
     * @return bool
     */
    public function insert($table, $data)
    {
        $params = [
            'index' => $this->dbName,
            'type' => $table,
            'body' => $this->convertToDBDataTypes($data)
        ];

        if (!empty($data['id']))
        {
            $params['id'] = $data['id'];
        }

        $response = $this->client->index($params);
        if ($response['result'] != 'created' && $response['result'] != 'updated')
        {
            Debugger::log($response, Debugger::ERROR);
            return false;
        }
        return true;
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

        $generalParams = [
            'index' => [
                '_index' => $this->dbName,
                '_type' => $table
            ]
        ];

        $params = [];
        foreach ($data as $row)
        {
            if (isset($row['id']))
            {
                $generalParams['index']['_id'] = $row['id'];
            }
            else
            {
                unset($generalParams['index']['_id']);
            }
            $params['body'][] = $generalParams;

            $params['body'][] = $this->convertToDBDataTypes($row);
        }

        $responses = $this->client->bulk($params);
        if ($responses['errors'] !== false)
        {
            if (!empty($responses['items']))
            {
                Debugger::log($responses['items'], Debugger::ERROR);
            }
            throw new DBException(self::ERROR_BULK_INSERT_ERROR);
        }
        return true;
    }

    /**
     * @param string $table
     * @param int $id
     *
     * @return array|bool
     * @throws DBException
     * @throws \Exception
     */
    public function get($table, $id)
    {
        $params = [
            'index' => $this->dbName,
            'type' => $table,
            'id' => $id
        ];

        try
        {
            $response = $this->client->get($params);
            if ($response['found'] === true)
            {
                return $this->convertFromDBDataTypes($response['_source']);
            }
        }
        catch(\Exception $e)
        {
            if (strpos($e->getMessage(), '"found":false') !== false)
            {
                $this->checkIfTypeExists($params, $table, $e);
                return false;
            }
            $this->handleException($e);
        }
        return false;
    }

    /**
     * @param string $table
     * @param int $id
     * @param array $data
     *
     * @return bool|mixed
     * @throws DBException
     * @throws \Exception
     */
    public function update($table, $id, $data)
    {
        $params = [
            'index' => $this->dbName,
            'type' => $table,
            'id' => $id,
            'body' => [
                'doc' => $this->convertToDBDataTypes($data)
            ]
        ];

        try
        {
            $response = $this->client->update($params);
            if ($response['result'] == 'updated')
            {
                return true;
            }
        }
        catch(\Exception $e)
        {
            if (strpos($e->getMessage(), '"type":"document_missing_exception"') !== false)
            {
                $this->checkIfTypeExists($params, $table, $e);
                throw new DBException(str_replace('{$id}', $id, self::ERROR_UPDATE));
            }
            else
            {
                $this->handleException($e);
            }
        }
        return false;
    }

    /**
     * @param string $table
     * @param int $id
     *
     * @return bool|mixed
     * @throws DBException
     * @throws \Exception
     */
    public function delete($table, $id)
    {
        $params = [
            'index' => $this->dbName,
            'type' => $table,
            'id' => $id
        ];

        try
        {
            $response = $this->client->delete($params);
            if ($response['result'] == 'deleted')
            {
                return true;
            }
        }
        catch(\Exception $e)
        {
            if (strpos($e->getMessage(), '"found":false') !== false)
            {
                $this->checkIfTypeExists($params, $table, $e);
                throw new DBException(str_replace('{$id}', $id, self::ERROR_DELETE));
            }
            else
            {
                $this->handleException($e);
            }
        }
        return false;
    }

    /**
     * @param string $table
     *
     * @return bool
     * @throws DBException
     */
    public function deleteAll($table)
    {
        $params = [
            'index' => $this->dbName,
            'type' => $table,
            'body' => [
                'query' => [
                    'match_all' => new \stdClass()
                ]
            ]
        ];
        $this->checkIfTypeExists($params, $table, new DBException());
        $this->client->deleteByQuery($params);
        return true;
    }

    /**
     * @param string $table
     * @param array $params
     *
     * @return array|bool|int
     * @throws DBException
     * @throws \Exception
     */
    public function findBy($table, $params)
    {
        $params = $this->checkParams($params);
        $paramsES = [
            'index' => $this->dbName,
            'type' => $table,
            'body' => []
        ];

        if (!empty($params[DB::PARAM_FIELDS]))
        {
            $paramsES['_source_include'] = implode(',', $params[DB::PARAM_FIELDS]);
        }
        if (!empty($params[DB::PARAM_WHERE]))
        {
            $conditions = [];
            foreach ($params[DB::PARAM_WHERE] as $condition => $values)
            {
                $values = $this->convertToDBDataTypes($values);
                if (count($params[DB::PARAM_WHERE]) === 1)
                {
                    $conditions = $this->parseBooleanQuery($this->putValuesIntoQuery($condition, $values));
                }
                else
                {
                    $conditions['bool']['filter'][] = $this->parseBooleanQuery($this->putValuesIntoQuery($condition, $values));
                }
            }
            $paramsES['body']['query'] = $conditions;
        }
        if (!empty($params[DB::PARAM_LIMIT]))
        {
            if (empty($params[DB::PARAM_GROUP_BY]))
            {
                $paramsES['body']['size'] = $params[DB::PARAM_LIMIT];
                if (!empty($params[DB::PARAM_OFFSET]))
                {
                    $paramsES['body']['from'] = $params[DB::PARAM_OFFSET];
                }
            }
            else
            {
                $limit = $params[DB::PARAM_LIMIT];
                if (!empty($params[DB::PARAM_OFFSET]))
                {
                    $limit += $params[DB::PARAM_OFFSET];
                }
                $paramsES['body']['aggs']['group_by']['terms']['size'] = $limit;
            }
        }
        elseif (!empty($params[DB::PARAM_GROUP_BY]))
        {
            // Aggregation - nechceme normální výsledky
            $paramsES['body']['size'] = 0;
            $paramsES['body']['aggs']['group_by']['terms']['size'] = self::DEFAULT_LIMIT;
        }
        elseif (empty($params[DB::PARAM_COUNT]))
        {
            // Default limit
            $paramsES['body']['size'] = self::DEFAULT_LIMIT;
        }
        if (!empty($params[DB::PARAM_ORDER_BY]) && empty($params[DB::PARAM_GROUP_BY]) && empty($params[DB::PARAM_AGGREGATION]))
        {
            foreach ($params[DB::PARAM_ORDER_BY] as $column)
            {
                $desc = strpos($column, ' desc');
                if ($desc !== false)
                {
                    $column = substr($column, 0, $desc);
                }
                $paramsES['body']['sort'][] = [$column => ($desc !== false ? 'desc' : 'asc')];
            }
            $paramsES['body']['sort'][] = "_score";
        }
        if (!empty($params[DB::PARAM_GROUP_BY]))
        {
            // Je možné v groupBy uvést "script" a definovat skript v groupByScript
            if ($params[DB::PARAM_GROUP_BY] !== 'script' || empty($params[DB::PARAM_GROUP_BY_SCRIPT]))
            {
                $paramsES['body']['aggs']['group_by']['terms']['field'] = $params[DB::PARAM_GROUP_BY];
            }
            else
            {
                $paramsES['body']['aggs']['group_by']['terms']['script'] = $params[DB::PARAM_GROUP_BY_SCRIPT];
            }
            if (!empty($params[DB::PARAM_ORDER_BY]))
            {
                $paramsES['body']['aggs']['group_by']['aggs']['results']['top_hits']['size'] = 1;
                if (!empty($params[DB::PARAM_FIELDS]))
                {
                    // Chceme vrátit jen požadovaná pole
                    $paramsES['body']['aggs']['group_by']['aggs']['results']['top_hits']['_source']['includes'] = $params[DB::PARAM_FIELDS];
                }
                foreach ($params[DB::PARAM_ORDER_BY] as $column)
                {
                    $desc = strpos($column, ' desc');
                    if ($desc !== false)
                    {
                        $column = substr($column, 0, $desc);
                    }
                    $paramsES['body']['aggs']['group_by']['terms']['order'][] = [($column === $params[DB::PARAM_GROUP_BY] ? '_key' : ($column === DB::PARAM_COUNT ? '_count' : $column)) => ($desc !== false ? 'desc' : 'asc')];
                    // Musí se přidat agregace podle sloupce, podle kterého chceme řadit
                    if ($column !== $params[DB::PARAM_GROUP_BY] && $column !== DB::PARAM_COUNT)
                    {
                        $paramsES['body']['aggs']['group_by']['aggs'][$column][$desc !== false ? 'max' : 'min']['field'] = $column;
                    }
                }
                // Vnitřní řazení v GROUP BY buckets (jaký záznam ze skupiny chceme)
                if (!empty($params[DB::PARAM_GROUP_INTERNAL_ORDER_BY]))
                {
                    foreach ($params[DB::PARAM_GROUP_INTERNAL_ORDER_BY] as $column)
                    {
                        $desc = strpos($column, ' desc');
                        if ($desc !== false)
                        {
                            $column = substr($column, 0, $desc);
                        }
                        $paramsES['body']['aggs']['group_by']['aggs']['results']['top_hits']['sort'][] = [$column => ['order' => $desc !== false ? 'desc' : 'asc']];
                    }
                }
            }
        }
        if (!empty($params[DB::PARAM_AGGREGATION]))
        {
            foreach ($params[DB::PARAM_AGGREGATION] as $agg => $columns)
            {
                if (empty($params[DB::PARAM_GROUP_BY]))
                {
                    foreach ($columns as $column)
                    {
                        $paramsES['body']['aggs']["{$agg}_{$column}"][$agg] = ["field" => $column];
                    }
                }
                else
                {
                    foreach ($columns as $column)
                    {
                        $paramsES['body']['aggs']['group_by']['aggs']["{$agg}_{$column}"][$agg] = ["field" => $column];
                    }
                }
            }
        }
        if (!empty($params[DB::PARAM_COUNT]) && empty($params[DB::PARAM_GROUP_BY]))
        {
            try
            {
                $results = $this->client->count($paramsES);
                return $results[DB::PARAM_COUNT];
            }
            catch(\Exception $e)
            {
                $this->handleException($e);
            }
        }

        try
        {
            $results = $this->client->search($paramsES);
            $finalResults = [];
            if (!empty($params[DB::PARAM_GROUP_BY]))
            {
                $buckets = $results['aggregations']['group_by']['buckets'];
                // Pokud zjišťujeme pouze počet záznamů, zajímá nás počet buckets
                if (!empty($params[DB::PARAM_COUNT]))
                {
                    $count = count($buckets);
                    // Pokud nebylo nic nalezeno, zjistíme, jestli existuje požadovaný typ
                    if ($count === 0)
                    {
                        $this->checkIfTypeExists($paramsES, $table, new DBException());
                    }
                    return $count;
                }
                foreach ($buckets as $bucket)
                {
                    if (!empty($bucket['results']))
                    {
                        if (self::DEFAULT_GROUP_LIMIT !== 1)
                        {
                            throw new NotImplementedException("Vracení jiného počtu výsledků než 1 pro skupinu (v GROUP BY) je třeba doimplementovat.");
                        }
                        // Výsledky s top_hits
                        $result = $bucket['results']['hits']['hits'][0]['_source'];
                    }
                    else
                    {
                        $result = [$params[DB::PARAM_GROUP_BY] => $bucket['key'], 'count' => $bucket['doc_count']];
                    }
                    if (!empty($params[DB::PARAM_AGGREGATION]))
                    {
                        foreach ($params[DB::PARAM_AGGREGATION] as $agg => $columns)
                        {
                            foreach ($columns as $column)
                            {
                                $result["{$agg}_{$column}"] = $bucket["{$agg}_{$column}"]['value'];
                            }
                        }
                    }
                    $finalResults[] = $this->convertFromDBDataTypes($result);
                }
                if (!empty($params[DB::PARAM_OFFSET]))
                {
                    $finalResults = array_slice($finalResults, $params[DB::PARAM_OFFSET]);
                }
            }
            elseif (!empty($params[DB::PARAM_AGGREGATION]))
            {
                $result = [];
                foreach ($params[DB::PARAM_AGGREGATION] as $agg => $columns)
                {
                    foreach ($columns as $column)
                    {
                        $result["{$agg}_{$column}"] = $results['aggregations']["{$agg}_{$column}"]['value'];
                    }
                }
                $finalResults[] = $this->convertFromDBDataTypes($result);
            }
            else
            {
                foreach ($results['hits']['hits'] as $result)
                {
                    $row = $result['_source'];
                    if (!empty($params[DB::PARAM_FIELDS]) && in_array('id', $params[DB::PARAM_FIELDS]))
                    {
                        $row = array_merge(['id' => $result['_id']], $row);
                    }
                    $finalResults[] = $this->convertFromDBDataTypes($row);
                }
            }
            // Pokud nejsou výsledky, zjistíme, jestli existuje požadovaný typ
            if (empty($finalResults))
            {
                $this->checkIfTypeExists($paramsES, $table, new DBException());
            }
            return $finalResults;
        }
        catch(\Exception $e)
        {
            $this->handleException($e);
        }
        return false;
    }

    /**
     * @param array $data
     *
     * @return array
     */
	public function convertToDBDataTypes($data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => $value)
            {
                if ($value instanceof \DateTime)
                {
                    $data[$key] = $value->format('c');
                }
                elseif (is_array($value))
                {
                    $data[$key] = $this->convertToDBDataTypes($value);
                }
            }
        }
        return $data;
    }

    /**
     * @param array $data
     *
     * @return array
     */
	public function convertFromDBDataTypes($data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => $value)
            {
                if (is_array($value))
                {
                    $data[$key] = $this->convertFromDBDataTypes($value);
                }
                // Konverze na DateTime
                elseif (strlen($value) == 10 && preg_match('/[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]/', $value) == 1 ||
                        preg_match('/[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]/', $value) == 1)
                {
                    $data[$key] = new \DateTime($value);
                }
            }
        }
        return $data;
    }

    /**
     * @param $result
     * @param $clause
     */
    protected function addAndClause(&$result, $clause)
    {
        $result['bool']['filter'][] = $clause;
    }

    /**
     * @param $result
     * @param $clause
     */
    protected function addOrClause(&$result, $clause)
    {
        $result['bool']['should'][] = $clause;
    }

    /**
     * Rozparsuje booleovský výraz bez závorek (jen AND a OR).
     *
     * @param $query
     *
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
                    $partialResult[] = $this->parseExpression($expr);
                }
                if (count($ands) === 1)
                {
                    $result['bool']['filter'] = $partialResult;
                }
                else
                {
                    $result['bool']['should'][]['bool']['filter'] = $partialResult;
                }
            }
        }
        return $result;
    }

    /**
     * Rozparsuje výraz proměnná >,<,=,>=,<=,!= hodnota.
     *
     * @param $expr
     *
     * @return mixed
     */
    protected function parseExpression($expr)
    {
        if (($pos = strpos($expr, '<=')) !== false)
        {
            $val = trim(substr($expr, $pos + 2));
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['lte'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '>=')) !== false)
        {
            $val = trim(substr($expr, $pos + 2));
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['gte'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '!=')) !== false)
        {
            $val = trim(substr($expr, $pos + 2));
            $result['bool']['must_not']['match'][trim(substr($expr, 0, $pos))] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '<')) !== false)
        {
            $val = trim(substr($expr, $pos + 1));
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['lt'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '>')) !== false)
        {
            $val = trim(substr($expr, $pos + 1));
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['gt'] = is_numeric($val) ? (int) $val : $val;
            return $result;
        }
        if (($pos = strpos($expr, '=')) !== false)
        {
            $val = trim(substr($expr, $pos + 1));
            // Je možné zde dát pole, zadané takto [DB::PARAM_WHERE]['sloupec = ?'] = [1, 2, 3];
            if (($start = strpos($val, '[')) !== false && ($end = strpos($val, ']')) !== false && $start < $end)
            {
                $val = explode(',', substr($val, $start + 1, $end - 1));
                foreach ($val as $key => $item)
                {
                    if (intval($item) == $item)
                    {
                        $val[$key] = intval($item);
                    }
                }
                $result['terms'][trim(substr($expr, 0, $pos))] = $val;
            }
            else
            {
                $result['match'][trim(substr($expr, 0, $pos))] = is_numeric($val) ? (int) $val : $val;
            }
            return $result;
        }
        if (($pos = strpos($expr, ' LIKE ')) !== false)
        {
            $val = trim(mb_substr($expr, $pos + 6));
            $result['wildcard'][trim(mb_substr($expr, 0, $pos))] = mb_strtolower(mb_ereg_replace("%", "*", $val));
            return $result;
        }
        if (($pos = strpos($expr, ' IS NULL')) !== false)
        {
            $result['bool']['must_not']['exists']['field'] = trim(mb_substr($expr, 0, $pos));
            return $result;
        }
        if (($pos = strpos($expr, ' IS NOT NULL')) !== false)
        {
            $result['exists']['field'] = trim(mb_substr($expr, 0, $pos));
            return $result;
        }
        return $expr;
    }

    /**
     * Zjištění jestli typ existuje.
     *
     * @param array $params
     * @param string $table
     * @param \Exception $e
     *
     * @throws DBException
     */
    private function checkIfTypeExists($params, $table, $e)
    {
        $data['index'] = $params['index'];
        $data['type'] = $params['type'];
        $exists = $this->client->indices()->existsType($data);
        if (!$exists) {
            throw new DBException(
                str_replace('{$db}', $this->dbName,
                    str_replace('{$table}', $table, self::ERROR_TABLE_DOESNT_EXIST)
                ), $e->getCode());
        }
    }

    /**
     * Odchycení společných výjimek.
     *
     * @param $e \Exception
     *
     * @throws DBException
     * @throws \Exception
     */
    private function handleException($e)
    {
        if (strpos($e->getMessage(), '"type":"index_not_found_exception"') !== false)
        {
            throw new DBException(str_replace('{$db}', $this->dbName, self::ERROR_DB_DOESNT_EXIST), $e->getCode());
        }
        throw $e;
    }
}