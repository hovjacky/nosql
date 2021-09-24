<?php

namespace Hovjacky\NoSQL;

use DateTime;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use stdClass;
use Throwable;
use Tracy\Debugger;

/**
 * Class ElasticsearchClient
 * @package Hovjacky\NoSQL
 */
class ElasticsearchClient extends DBWithBooleanParsing
{
    /** @var Client */
    private $client;

    /** @var int výchozí limit vrácených položek z elasticu */
    public const DEFAULT_LIMIT = 100;

    /** @var int Tímto se to bude chovat jako SQL, vrátí jeden záznam pro jednu GROUP BY hodnotu, ale dalo by se nastavit i jinak. */
    public const DEFAULT_GROUP_LIMIT = 1;


    /**
     * ElasticsearchClient constructor.
     * @param array $params
     */
    public function __construct($params)
    {
        $withAuth = !empty($params['username']) && !empty($params['password']);

        $connectionParams[] = ($withAuth ? "https://{$params['username']}:{$params['password']}@" : '') . $params['host'] . (!empty($params['port']) ? ':' . $params['port'] : '');

        $clientBuilder = ClientBuilder::create()->setHosts($connectionParams);

        if (!empty($params['disableSslVerification']))
        {
            $clientBuilder->setSSLVerification(FALSE);
        }

        $this->client = $clientBuilder->build();
        $this->dbName = $params['index'];
    }


    /**
     * Vrací klient připojení do elasticsearch.
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }


    /**
     * Vložení nových informací do elasticsearch.
     * @param string $table
     * @param array $data
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

        if ($response['result'] !== 'created' && $response['result'] !== 'updated')
        {
            Debugger::log($response, Debugger::ERROR);

            return false;
        }

        return true;
    }


    /**
     * Hromadné vložení informací do elasticsearch.
     * @param string $table
     * @param array $data
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
     * Načtení informací o položce z elasticsearch.
     * @param string $table
     * @param int $id
     * @return array|false
     * @throws DBException
     * @throws Throwable
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
        catch(Throwable $e)
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
     * Aktualizace informací v elasticsearch.
     * @param string $table
     * @param int $id
     * @param array $data
     * @return bool|mixed
     * @throws DBException
     * @throws Throwable
     */
    public function update($table, $id, $data)
    {
        $params = [
            'index' => $this->dbName,
            //'type' => $table,
            'id' => $id,
            'body' => [
                'doc' => $this->convertToDBDataTypes($data)
            ]
        ];

        try
        {
            $response = $this->client->update($params);

            if ($response['result'] === 'updated')
            {
                return true;
            }
        }
        catch(Throwable $e)
        {
            if (strpos($e->getMessage(), '"type":"document_missing_exception"') !== false)
            {
                $this->checkIfTypeExists($params, $table, $e);

                throw new DBException(str_replace('{$id}', (string) $id, self::ERROR_UPDATE));
            }
            else
            {
                $this->handleException($e);
            }
        }

        return false;
    }


    /**
     * Smazání záznamu z elasticsearch.
     * @param string $table
     * @param int $id
     * @return bool|mixed
     * @throws DBException
     * @throws Throwable
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

            if ($response['result'] === 'deleted')
            {
                return true;
            }
        }
        catch(Throwable $e)
        {
            if (strpos($e->getMessage(), '"found":false') !== false)
            {
                $this->checkIfTypeExists($params, $table, $e);

                throw new DBException(str_replace('{$id}', (string) $id, self::ERROR_DELETE));
            }
            else
            {
                $this->handleException($e);
            }
        }

        return false;
    }


    /**
     * Smazání všech záznamů z elasticsearch.
     * @param string $table
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
                    'match_all' => new stdClass()
                ]
            ]
        ];

        $this->checkIfTypeExists($params, $table, new DBException());
        $this->client->deleteByQuery($params);

        return true;
    }


    /**
     * Nalezení záznamu dle zadaných podmínek.
     * @param string $table
     * @param array $params
     * @return array|bool|int
     * @throws DBException
     * @throws Throwable
     */
    public function findBy($table, $params)
    {
        $params = $this->checkParams($params);

        $paramsES = [
            'index' => $this->dbName,
            'type' => $table,
            'body' => []
        ];

        if (!empty($params[self::PARAM_FIELDS]))
        {
            $paramsES['_source_include'] = implode(',', $params[self::PARAM_FIELDS]);
        }

        if (!empty($params[self::PARAM_WHERE]))
        {
            $conditions = [];

            foreach ($params[self::PARAM_WHERE] as $condition => $values)
            {
                $values = $this->convertToDBDataTypes($values);

                if (count($params[self::PARAM_WHERE]) === 1)
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

        if (!empty($params[self::PARAM_LIMIT]))
        {
            /** @noinspection NotOptimalIfConditionsInspection */
            if (empty($params[self::PARAM_GROUP_BY]))
            {
                $paramsES['body']['size'] = $params[self::PARAM_LIMIT];

                if (!empty($params[self::PARAM_OFFSET]))
                {
                    $paramsES['body']['from'] = $params[self::PARAM_OFFSET];
                }
            }
            else
            {
                $limit = $params[self::PARAM_LIMIT];

                if (!empty($params[self::PARAM_OFFSET]))
                {
                    $limit += $params[self::PARAM_OFFSET];
                }

                $paramsES['body']['aggs']['group_by']['terms']['size'] = $limit;
            }
        }
        elseif (!empty($params[self::PARAM_GROUP_BY]))
        {
            // Aggregation - nechceme normální výsledky
            $paramsES['body']['size'] = 0;
            $paramsES['body']['aggs']['group_by']['terms']['size'] = self::DEFAULT_LIMIT;
        }
        elseif (empty($params[self::PARAM_COUNT]))
        {
            // Default limit
            $paramsES['body']['size'] = self::DEFAULT_LIMIT;
        }

        if (!empty($params[self::PARAM_ORDER_BY]) && empty($params[self::PARAM_GROUP_BY]) && empty($params[self::PARAM_AGGREGATION]))
        {
            foreach ($params[self::PARAM_ORDER_BY] as $column)
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

        if (!empty($params[self::PARAM_GROUP_BY]))
        {
            // Je možné v groupBy uvést "script" a definovat skript v groupByScript
            if ($params[self::PARAM_GROUP_BY] !== 'script' || empty($params[self::PARAM_GROUP_BY_SCRIPT]))
            {
                $paramsES['body']['aggs']['group_by']['terms']['field'] = $params[self::PARAM_GROUP_BY];
            }
            else
            {
                $paramsES['body']['aggs']['group_by']['terms']['script'] = $params[self::PARAM_GROUP_BY_SCRIPT];
            }
            if (!empty($params[self::PARAM_ORDER_BY]))
            {
                $paramsES['body']['aggs']['group_by']['aggs']['results']['top_hits']['size'] = 1;

                if (!empty($params[self::PARAM_FIELDS]))
                {
                    // Chceme vrátit jen požadovaná pole
                    $paramsES['body']['aggs']['group_by']['aggs']['results']['top_hits']['_source']['includes'] = $params[self::PARAM_FIELDS];
                }

                foreach ($params[self::PARAM_ORDER_BY] as $column)
                {
                    $desc = strpos($column, ' desc');

                    if ($desc !== false)
                    {
                        $column = substr($column, 0, $desc);
                    }

                    $paramsES['body']['aggs']['group_by']['terms']['order'][] = [($column === $params[self::PARAM_GROUP_BY] ? '_key' : ($column === self::PARAM_COUNT ? '_count' : $column)) => ($desc !== false ? 'desc' : 'asc')];

                    // Musí se přidat agregace podle sloupce, podle kterého chceme řadit
                    if ($column !== $params[self::PARAM_GROUP_BY] && $column !== self::PARAM_COUNT)
                    {
                        $paramsES['body']['aggs']['group_by']['aggs'][$column][$desc !== false ? 'max' : 'min']['field'] = $column;
                    }
                }

                // Vnitřní řazení v GROUP BY buckets (jaký záznam ze skupiny chceme)
                if (!empty($params[self::PARAM_GROUP_INTERNAL_ORDER_BY]))
                {
                    foreach ($params[self::PARAM_GROUP_INTERNAL_ORDER_BY] as $column)
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

        if (!empty($params[self::PARAM_AGGREGATION]))
        {
            foreach ($params[self::PARAM_AGGREGATION] as $agg => $columns)
            {
                if (empty($params[self::PARAM_GROUP_BY]))
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

        if (!empty($params[self::PARAM_COUNT]) && empty($params[self::PARAM_GROUP_BY]))
        {
            try
            {
                $results = $this->client->count($paramsES);

                return $results[self::PARAM_COUNT];
            }
            catch(Throwable $e)
            {
                $this->handleException($e);
            }
        }

        try
        {
            $results = $this->client->search($paramsES);

            $finalResults = [];

            if (!empty($params[self::PARAM_GROUP_BY]))
            {
                $buckets = $results['aggregations']['group_by']['buckets'];

                // Pokud zjišťujeme pouze počet záznamů, zajímá nás počet buckets
                if (!empty($params[self::PARAM_COUNT]))
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
                        // @phpstan-ignore-next-line
                        if (self::DEFAULT_GROUP_LIMIT !== 1)
                        {
                            throw new NotImplementedException("Vracení jiného počtu výsledků než 1 pro skupinu (v GROUP BY) je třeba doimplementovat.");
                        }

                        // Výsledky s top_hits
                        $result = $bucket['results']['hits']['hits'][0]['_source'];
                    }
                    else
                    {
                        $result = [$params[self::PARAM_GROUP_BY] => $bucket['key'], 'count' => $bucket['doc_count']];
                    }

                    /** @noinspection NotOptimalIfConditionsInspection */
                    if (!empty($params[self::PARAM_AGGREGATION]))
                    {
                        foreach ($params[self::PARAM_AGGREGATION] as $agg => $columns)
                        {
                            foreach ($columns as $column)
                            {
                                $result["{$agg}_{$column}"] = $bucket["{$agg}_{$column}"]['value'];
                            }
                        }
                    }

                    $finalResults[] = $this->convertFromDBDataTypes($result);
                }

                if (!empty($params[self::PARAM_OFFSET]))
                {
                    $finalResults = array_slice($finalResults, $params[self::PARAM_OFFSET]);
                }
            }
            elseif (!empty($params[self::PARAM_AGGREGATION]))
            {
                $result = [];

                foreach ($params[self::PARAM_AGGREGATION] as $agg => $columns)
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

                    if (!empty($params[self::PARAM_FIELDS]) && in_array('id', $params[self::PARAM_FIELDS]))
                    {
                        /** @noinspection SlowArrayOperationsInLoopInspection */
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
        catch(Throwable $e)
        {
            $this->handleException($e);
        }

        return false;
    }


    /**
     * Převedení datových typů DB.
     * @param array $data
     * @return array
     */
	public function convertToDBDataTypes($data)
    {
        if (is_array($data))
        {
            foreach ($data as $key => $value)
            {
                if ($value instanceof DateTime)
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
     * Převedení datových typů z DB.
     * @param array $data
     * @return array
     * @throws Throwable
     * @noinspection NotOptimalIfConditionsInspection
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
                elseif (
                    (strlen($value) == 10 && preg_match('/[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]/', $value) === 1)
                    || preg_match('/[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]/', $value) === 1)
                {
                    $data[$key] = new DateTime($value);
                }
            }
        }

        return $data;
    }


    /**
     * Přidá $clause do $result jako and.
     * @param array $result
     * @param array $clause
     * @return void
     */
    protected function addAndClause(&$result, $clause)
    {
        $result['bool']['filter'][] = $clause;
    }


    /**
     * Přidá $clause do $result jako or.
     * @param array $result
     * @param array $clause
     * @return void
     */
    protected function addOrClause(&$result, $clause)
    {
        $result['bool']['should'][] = $clause;
    }


    /**
     * Rozparsuje booleovský výraz bez závorek (jen AND a OR).
     * @param string $query
     * @return array
     * @throws DBException
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

                break;
            }

            $partialResult = [];

            foreach ($exprs as $expr)
            {
                $partialResult[] = $this->parseExpression($expr);
            }

            /** @noinspection NotOptimalIfConditionsInspection */
            if (count($ands) === 1)
            {
                $result['bool']['filter'] = $partialResult;
            }
            else
            {
                $result['bool']['should'][]['bool']['filter'] = $partialResult;
            }
        }

        return $result;
    }


    /**
     * Rozparsuje výraz proměnná >,<,=,>=,<=,!= hodnota.
     * @param string $expr
     * @return array
     * @throws DBException
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

            // Je možné zde dát pole, zadané takto [self::PARAM_WHERE]['sloupec = ?'] = [1, 2, 3];
            if (($start = strpos($val, '[')) !== false && ($end = strpos($val, ']')) !== false && $start < $end)
            {
                $val = explode(',', substr($val, $start + 1, $end - 1));

                foreach ($val as $key => $item)
                {
                    if ((int) $item == $item)
                    {
                        $val[$key] = (int) $item;
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
            $result['wildcard'][trim(mb_substr($expr, 0, $pos))] = mb_strtolower((string) mb_ereg_replace("%", "*", $val));

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

        if (($pos = strpos($expr, ' CROSS FIELDS ')) !== false)
        {
            $fields = trim(mb_substr($expr, 0, $pos));
            $val = trim(substr($expr, $pos + 14));

            $result['multi_match'] = [
                'query' => $val,
                'type' => 'cross_fields',
                'operator' => 'and',
                "fields" => explode(",", $fields)
            ];

            return $result;
        }

        throw new DBException('No expression matched.');
    }


    /**
     * Zjištění jestli typ existuje.
     * @param array $params
     * @param string $table
     * @param Throwable $e
     * @return void
     * @throws DBException
     */
    private function checkIfTypeExists($params, $table, $e)
    {
        $data['index'] = $params['index'];
        $data['type'] = $params['type'];

        $exists = $this->client->indices()->existsType($data);

        if (!$exists)
        {
            throw new DBException(
                str_replace('{$db}', $this->dbName,
                    str_replace('{$table}', $table, self::ERROR_TABLE_DOESNT_EXIST)
                ), $e->getCode());
        }
    }


    /**
     * Odchycení společných výjimek.
     * @param Throwable $e
     * @return void
     * @throws DBException
     * @throws Throwable
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
