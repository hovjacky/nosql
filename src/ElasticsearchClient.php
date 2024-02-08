<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use DateTime;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Throwable;
use Tracy\Debugger;
use stdClass;

/**
 * Class ElasticsearchClient
 * @package Hovjacky\NoSQL
 */
class ElasticsearchClient extends DBWithBooleanParsing
{
    /** @var Client */
    private Client $client;


    /** @var int výchozí limit vrácených položek z elasticu */
    public const DEFAULT_LIMIT = 100;

    /** @var int Tímto se to bude chovat jako SQL, vrátí jeden záznam pro jednu GROUP BY hodnotu, ale dalo by se nastavit i jinak. */
    public const DEFAULT_GROUP_LIMIT = 1;


    /**
     * ElasticsearchClient constructor.
     */
    public function __construct(array $params)
    {
        $withAuth = !empty($params['username']) && !empty($params['password']);

        $connectionParams[] = ($withAuth ? "https://{$params['username']}:{$params['password']}@" : '') . $params['host'] . (!empty($params['port']) ? ':' . $params['port'] : '');

        $clientBuilder = ClientBuilder::create()->setHosts($connectionParams);

        if (!empty($params['disableSslVerification']))
        {
            $clientBuilder->setSSLVerification(false);
        }

        $this->client = $clientBuilder->build();
    }


    /**
     * Vrací klient připojení do elasticsearch.
     * @return Client
     */
    public function getClient(): Client
    {
        return $this->client;
    }


    /**
     * Vložení nových informací do elasticsearch.
     * @return bool
     */
    public function insertOrUpdate(string $tableName, array $data): bool
    {
        $params = [
            'index' => $tableName,
            'body' => $this->convertToDBDataTypes($data),
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
     * @return bool
     * @throws DBException
     */
    public function bulkInsertOrUpdate(string $tableName, array $data): bool
    {
        if (empty($data))
        {
            throw new DBException(self::ERROR_BULK_INSERT);
        }

        $generalParams = [
            'index' => ['_index' => $tableName],
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
     * @throws DBException
     * @throws Throwable
     */
    public function get(string $tableName, int $id): ?array
    {
        $params = [
            'index' => $tableName,
            'id' => $id,
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
                return null;
            }

            $this->handleException($e);
        }

        return null;
    }


    /**
     * Aktualizace informací v elasticsearch.
     * @throws DBException
     * @throws Throwable
     */
    public function update(string $tableName, int $id, array $data): bool
    {
        $params = [
            'index' => $tableName,
            'id' => $id,
            'body' => [
                'doc' => $this->convertToDBDataTypes($data),
            ],
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
            if (str_contains($e->getMessage(), '"type":"document_missing_exception"'))
            {
                throw new DBException(str_replace('{$id}', (string) $id, self::ERROR_UPDATE));
            }

            $this->handleException($e);
        }

        return false;
    }


    /**
     * Smazání záznamu z elasticsearch.
     * @throws DBException
     * @throws Throwable
     */
    public function delete(string $tableName, int $id): bool
    {
        $params = [
            'index' => $tableName,
            'id' => $id,
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
            if (str_contains($e->getMessage(), '"found":false'))
            {
                throw new DBException(str_replace('{$id}', (string) $id, self::ERROR_DELETE));
            }

            $this->handleException($e);
        }

        return false;
    }


    /**
     * Smazání všech záznamů z elasticsearch.
     * @throws DBException
     */
    public function deleteAll(string $tableName): true
    {
        $params = [
            'index' => $tableName,
            'body' => [
                'query' => [
                    'match_all' => new stdClass(),
                ],
            ],
        ];

        $this->client->deleteByQuery($params);

        return true;
    }


    /**
     * @throws DBException
     * @throws Throwable
     */
    public function findBy(string $tableName, array $params): array|int
    {
        $params = $this->checkAndRepairParams($params);

        $paramsES = [
            'index' => $tableName,
            'body' => [],
        ];

        if (is_array($params[self::PARAM_FIELDS] ?? null) && !empty($params[self::PARAM_FIELDS]))
        {
            $paramsES['_source_includes'] = implode(',', $params[self::PARAM_FIELDS]);
        }

        if (is_array($params[self::PARAM_WHERE] ?? null) && !empty($params[self::PARAM_WHERE]))
        {
            $conditions = [];

            foreach ($params[self::PARAM_WHERE] as $condition => $values)
            {
                $values = $this->convertToDBDataTypes($values);
                $parsedBooleanQuery = $this->parseBooleanQuery($this->putValuesIntoQuery($condition, $values));

                if (count($params[self::PARAM_WHERE]) === 1)
                {
                    $conditions = $parsedBooleanQuery;

                    break;
                }

                $conditions['bool']['filter'][] = $parsedBooleanQuery;
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

        if (
            !empty($params[self::PARAM_ORDER_BY])
            && is_array($params[self::PARAM_ORDER_BY] ?? null)
            && empty($params[self::PARAM_GROUP_BY])
            && empty($params[self::PARAM_AGGREGATION])
        )
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

            $paramsES['body']['sort'][] = '_score';
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
            if (is_array($params[self::PARAM_ORDER_BY] ?? null) && !empty($params[self::PARAM_ORDER_BY]))
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
                if (
                    is_array($params[self::PARAM_GROUP_INTERNAL_ORDER_BY] ?? null)
                    && !empty($params[self::PARAM_GROUP_INTERNAL_ORDER_BY])
                )
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

        if (is_array($params[self::PARAM_AGGREGATION] ?? null) && !empty($params[self::PARAM_AGGREGATION]))
        {
            foreach ($params[self::PARAM_AGGREGATION] as $agg => $columns)
            {
                if (!is_array($columns))
                {
                    $columns = [$columns];
                }

                if (empty($params[self::PARAM_GROUP_BY]))
                {
                    foreach ($columns as $column)
                    {
                        $paramsES['body']['aggs']["{$agg}_{$column}"][$agg] = ['field' => $column];
                    }
                }
                else
                {
                    foreach ($columns as $column)
                    {
                        $paramsES['body']['aggs']['group_by']['aggs']["{$agg}_{$column}"][$agg] = ['field' => $column];
                    }
                }
            }
        }

        if (!empty($params[self::PARAM_COUNT]) && empty($params[self::PARAM_GROUP_BY]))
        {
            try
            {
                unset($paramsES['body']['size']);

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
                    return count($buckets);
                }

                foreach ($buckets as $bucket)
                {
                    if (!empty($bucket['results']))
                    {
                        // @phpstan-ignore-next-line
                        if (self::DEFAULT_GROUP_LIMIT !== 1)
                        {
                            throw new NotImplementedException(
                                'Vracení jiného počtu výsledků než 1 pro skupinu (v GROUP BY) je třeba doimplementovat.',
                            );
                        }

                        // Výsledky s top_hits
                        $result = $bucket['results']['hits']['hits'][0]['_source'];
                    }
                    else
                    {
                        $result = [$params[self::PARAM_GROUP_BY] => $bucket['key'], 'count' => $bucket['doc_count']];
                    }

                    /** @noinspection NotOptimalIfConditionsInspection */
                    if (is_array($params[self::PARAM_AGGREGATION] ?? null) && !empty($params[self::PARAM_AGGREGATION]))
                    {
                        foreach ($params[self::PARAM_AGGREGATION] as $agg => $columns)
                        {
                            if (!is_array($columns))
                            {
                                $columns = [$columns];
                            }
                            foreach ($columns as $column)
                            {
                                $result["{$agg}_{$column}"] = $bucket["{$agg}_{$column}"]['value'];
                            }
                        }
                    }

                    $finalResults[] = $this->convertFromDBDataTypes($result);
                }

                if (!empty($params[self::PARAM_OFFSET]) && is_numeric($params[self::PARAM_OFFSET]))
                {
                    $finalResults = array_slice($finalResults, (int) $params[self::PARAM_OFFSET]);
                }
            }
            elseif (is_array($params[self::PARAM_AGGREGATION] ?? null) && !empty($params[self::PARAM_AGGREGATION]))
            {
                $result = [];

                foreach ($params[self::PARAM_AGGREGATION] as $agg => $columns)
                {
                    if (!is_array($columns))
                    {
                        $columns = [$columns];
                    }
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

                    if (
                        is_array($params[self::PARAM_FIELDS] ?? null)
                        && !empty($params[self::PARAM_FIELDS])
                        && in_array('id', $params[self::PARAM_FIELDS])
                    )
                    {
                        /** @noinspection SlowArrayOperationsInLoopInspection */
                        $row = array_merge(['id' => $result['_id']], $row);
                    }

                    $finalResults[] = $this->convertFromDBDataTypes($row);
                }
            }

            return $finalResults;
        }
        catch(Throwable $e)
        {
            $this->handleException($e);
        }
    }


    public function convertToDBDataTypes(array $data): array
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

        return $data;
    }


    /**
     * @throws Throwable
     */
    public function convertFromDBDataTypes(array $data): array
    {
        foreach ($data as $key => $value)
        {
            if (is_array($value))
            {
                $data[$key] = $this->convertFromDBDataTypes($value);
            }
            // Konverze na DateTime
            elseif (
                is_string($value)
                && ((
                        strlen($value) === 10
                        && preg_match('/[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]/', $value) === 1
                    )
                    || preg_match('/[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]/', $value) === 1
                ))
            {
                $data[$key] = new DateTime($value);
            }
        }

        return $data;
    }


    /**
     * @param array<string, array<string, array>> $result
     */
    protected function addAndClause(array &$result, array $clause): void
    {
        $result['bool']['filter'][] = $clause;
    }


    /**
     * @param array<string, array<string, array>> $result
     */
    protected function addOrClause(array &$result, array $clause): void
    {
        $result['bool']['should'][] = $clause;
    }


    protected function parseAndOrQuery(string $query): array
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
     * @return array<string, mixed>
     * @throws DBException
     */
    protected function parseExpression(string $expr): array
    {
        if (($pos = strpos($expr, '<=')) !== false)
        {
            $val = trim(substr($expr, $pos + 2));
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['lte'] = is_numeric(
                $val,
            ) ? (int) $val : $val;

            return $result;
        }

        if (($pos = strpos($expr, '>=')) !== false)
        {
            $val = trim(substr($expr, $pos + 2));
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['gte'] = is_numeric(
                $val,
            ) ? (int) $val : $val;

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
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['lt'] = is_numeric(
                $val,
            ) ? (int) $val : $val;

            return $result;
        }

        if (($pos = strpos($expr, '>')) !== false)
        {
            $val = trim(substr($expr, $pos + 1));
            $result['bool']['filter'][]['range'][trim(substr($expr, 0, $pos))]['gt'] = is_numeric(
                $val,
            ) ? (int) $val : $val;

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
            $result['wildcard'][trim(mb_substr($expr, 0, $pos))] = mb_strtolower(
                (string) mb_ereg_replace('%', '*', $val),
            );

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
                'fields' => explode(',', $fields),
            ];

            return $result;
        }

        throw new DBException('No expression matched.');
    }


    /**
     * Odchycení společných výjimek.
     * @param Throwable $e
     * @throws DBException
     * @throws Throwable
     */
    private function handleException(Throwable $e): never
    {
        if (str_contains($e->getMessage(), '"type":"index_not_found_exception"'))
        {
            throw new DBException(self::ERROR_DB_DOESNT_EXIST, $e->getCode());
        }

        throw $e;
    }
}
