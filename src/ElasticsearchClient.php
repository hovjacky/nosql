<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use Closure;
use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;
use Elastic\Elasticsearch\Exception\ClientResponseException;
use Elastic\Elasticsearch\Response\Elasticsearch;
use Hovjacky\NoSQL\Query\FindByParams;
use Hovjacky\NoSQL\Query\SearchRequestBuilder;
use Hovjacky\NoSQL\Query\SearchResult;
use Hovjacky\NoSQL\Query\SearchResultMapper;
use Hovjacky\NoSQL\Type\DataTypeConverter;
use Http\Promise\Promise;
use Throwable;
use stdClass;

class ElasticsearchClient extends DBWithBooleanParsing
{
    private Client $client;

    /** @var callable(string $value): string|null */
    private $wildcardValueFilter = null;

    /** @var array<string, mixed[]> hodnoty seznamů právě parsované where podmínky (klíč = placeholder `#n#`) */
    private array $listPlaceholders = [];

    private ?SearchRequestBuilder $requestBuilder = null;

    private ?SearchResultMapper $resultMapper = null;

    private ?DataTypeConverter $dataTypeConverter = null;


    /** Výchozí limit vrácených položek z elasticu. */
    public const DEFAULT_LIMIT = 100;

    /** Tímto se to bude chovat jako SQL, vrátí jeden záznam pro jednu GROUP BY hodnotu, ale dalo by se nastavit i jinak. */
    public const DEFAULT_GROUP_LIMIT = 1;

    /** Virtuální parametr s daty bucketu z odpovědi z Elasticsearch. */
    public const PARAM_BUCKET = '__bucket';


    /**
     * ElasticsearchClient constructor.
     */
    public function __construct(array $params)
    {
        // Elasticsearch 8+ má security (TLS + autentizaci) ve výchozím stavu zapnutou
        // a přihlašovací údaje se předávají odděleně, ne inline v URL jako v 7.x.
        $scheme = !empty($params['scheme'])
            ? $params['scheme']
            : (!empty($params['username']) ? 'https' : 'http');

        $host = $scheme . '://' . $params['host'] . (!empty($params['port']) ? ':' . $params['port'] : '');

        $clientBuilder = ClientBuilder::create()->setHosts([$host]);

        if (!empty($params['username']) && !empty($params['password']))
        {
            $clientBuilder->setBasicAuthentication((string) $params['username'], (string) $params['password']);
        }

        if (!empty($params['apiKey']))
        {
            $clientBuilder->setApiKey((string) $params['apiKey']);
        }

        if (!empty($params['caCert']))
        {
            $clientBuilder->setCABundle((string) $params['caCert']);
        }

        if (!empty($params['disableSslVerification']))
        {
            $clientBuilder->setSSLVerification(false);
        }

        $this->client = $clientBuilder->build();
    }


    /**
     * Definice filtru hodnoty u wildcard filtru (= LIKE).
     * @param callable(string $value): string $wildcardValueFilter
     */
    public function setWildcardValueFilter(callable $wildcardValueFilter): void
    {
        $this->wildcardValueFilter = $wildcardValueFilter;
    }


    /**
     * Vrací klient připojení do elasticsearch.
     */
    public function getClient(): Client
    {
        return $this->client;
    }


    /**
     * Vložení nových informací do elasticsearch.
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

        $response = $this->responseToArray($this->client->index($params));

        if ($response['result'] !== 'created' && $response['result'] !== 'updated')
        {
            $this->logError('Elasticsearch nevrátil po zápisu záznamu očekávaný výsledek.', ['response' => $response]);

            return false;
        }

        return true;
    }


    /**
     * Hromadné vložení informací do elasticsearch.
     * @throws DBException
     */
    public function bulkInsertOrUpdate(string $tableName, array $data, bool $waitForDataRefresh = false): bool
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

        if ($waitForDataRefresh)
        {
            $params['refresh'] = 'wait_for';
        }

        $responses = $this->responseToArray($this->client->bulk($params));

        if ($responses['errors'] !== false)
        {
            if (!empty($responses['items']))
            {
                $this->logError('Chyby při hromadném zápisu do Elasticsearch.', ['items' => $responses['items']]);
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
    public function get(string $tableName, string|int $id): ?array
    {
        $params = [
            'index' => $tableName,
            'id' => (string) $id,
        ];

        try
        {
            $response = $this->responseToArray($this->client->get($params));

            if (($response['found'] ?? false) === true)
            {
                return $this->convertFromDBDataTypes($response['_source']);
            }
        }
        catch(ClientResponseException $e)
        {
            // Nenalezený dokument vrací v Elasticsearch 8+ HTTP 404.
            if ($e->getCode() === 404)
            {
                return null;
            }

            $this->handleException($e);
        }
        catch(Throwable $e)
        {
            $this->handleException($e);
        }

        return null;
    }


    /**
     * Aktualizace informací v elasticsearch.
     * @throws DBException
     * @throws Throwable
     */
    public function update(string $tableName, string|int $id, array $data): bool
    {
        $params = [
            'index' => $tableName,
            'id' => (string) $id,
            'body' => [
                'doc' => $this->convertToDBDataTypes($data),
            ],
        ];

        try
        {
            $response = $this->responseToArray($this->client->update($params));

            if ($response['result'] === 'updated')
            {
                return true;
            }
        }
        catch(ClientResponseException $e)
        {
            if ($this->getElasticErrorType($e) === 'document_missing_exception')
            {
                throw DBException::recordNotUpdated($id);
            }

            $this->handleException($e);
        }
        catch(Throwable $e)
        {
            $this->handleException($e);
        }

        return false;
    }


    /**
     * Smazání záznamu z elasticsearch.
     * @throws DBException
     * @throws Throwable
     */
    public function delete(string $tableName, string|int $id): bool
    {
        $params = [
            'index' => $tableName,
            'id' => (string) $id,
        ];

        try
        {
            $response = $this->responseToArray($this->client->delete($params));

            if ($response['result'] === 'deleted')
            {
                return true;
            }
        }
        catch(ClientResponseException $e)
        {
            // Mazání neexistujícího dokumentu vrací v Elasticsearch 8+ HTTP 404
            // (neexistující index má naopak error type index_not_found_exception).
            if ($e->getCode() === 404 && $this->getElasticErrorType($e) !== 'index_not_found_exception')
            {
                throw DBException::recordNotDeleted($id);
            }

            $this->handleException($e);
        }
        catch(Throwable $e)
        {
            $this->handleException($e);
        }

        return false;
    }


    /**
     * Smazání všech záznamů z elasticsearch.
     * @throws DBException
     */
    public function deleteAll(string $tableName): void
    {
        $params = [
            'index' => $tableName,
            // Konflikty verzí (souběžná změna dokumentu) ignorujeme a mažeme dál – sémantika "smaž vše".
            // V Elasticsearch 8+ by jinak delete_by_query s konfliktem vrátil HTTP 409 a klient by vyhodil výjimku.
            'conflicts' => 'proceed',
            'body' => [
                'query' => [
                    'match_all' => new stdClass(),
                ],
            ],
        ];

        $this->client->deleteByQuery($params);
    }


    /**
     * Zjistí, zda index existuje.
     * @throws Throwable
     */
    public function indexExists(string $tableName): bool
    {
        try
        {
            $response = $this->client->indices()->exists(['index' => $tableName]);
            // V synchronním režimu vrací klient vždy Elasticsearch, nikdy Promise.
            assert($response instanceof Elasticsearch);

            return $response->asBool();
        }
        catch(ClientResponseException $e)
        {
            if ($e->getCode() === 404)
            {
                return false;
            }

            throw $e;
        }
    }


    /**
     * Smaže index. Pokud index neexistuje, nic se neděje.
     * @throws DBException
     * @throws Throwable
     */
    public function deleteIndex(string $tableName): void
    {
        try
        {
            $this->client->indices()->delete(['index' => $tableName]);
        }
        catch(ClientResponseException $e)
        {
            // Neexistující index při mazání ignorujeme.
            if ($e->getCode() !== 404)
            {
                $this->handleException($e);
            }
        }
    }


    /**
     * Vytvoří index podle zadané definice (settings + mappings).
     * @param array<string, mixed> $definition tělo požadavku pro vytvoření indexu
     */
    public function createIndex(string $tableName, array $definition): void
    {
        $this->client->indices()->create([
            'index' => $tableName,
            'body' => $definition,
        ]);
    }


    /**
     * Vrátí záznamy odpovídající daným kritériím, nebo jejich počet (s parametrem `count`).
     *
     * @param array<mixed>|null $resultData Surová odpověď z Elasticsearch (předává se referencí).
     *      @deprecated Použijte search(), která vrací SearchResult i se surovou odpovědí.
     * @throws DBException
     * @throws Throwable
     */
    public function findBy(
        string $tableName,
        array $params,
        ?callable $modifyParamsCallback = null,
        ?array &$resultData = null
    ): array|int
    {
        $findByParams = FindByParams::fromArray($this->checkAndRepairParams($params));

        // Počet záznamů bez GROUP BY umí Elasticsearch vrátit rovnou přes `_count`.
        if ($findByParams->count && $findByParams->groupBy === null)
        {
            return $this->executeCount($tableName, $findByParams, $modifyParamsCallback);
        }

        $resultData = $response = $this->executeSearch($tableName, $findByParams, $modifyParamsCallback);

        return $this->getResultMapper()->map($response, $findByParams, $this->convertFromDBDataTypes(...));
    }


    /**
     * Vyhledá záznamy a vrátí je i se surovou odpovědí z Elasticsearch.
     *
     * Na rozdíl od findBy() vrací vždy záznamy - parametr `count` se ignoruje,
     * od zjišťování počtu je metoda count().
     * @param array<string, mixed> $params
     * @throws DBException
     * @throws Throwable
     */
    public function search(string $tableName, array $params, ?callable $modifyParamsCallback = null): SearchResult
    {
        unset($params[self::PARAM_COUNT]);

        $findByParams = FindByParams::fromArray($this->checkAndRepairParams($params));
        $response = $this->executeSearch($tableName, $findByParams, $modifyParamsCallback);
        $rows = $this->getResultMapper()->map($response, $findByParams, $this->convertFromDBDataTypes(...));

        return new SearchResult(is_array($rows) ? $rows : [], self::totalHits($response), $response);
    }


    /**
     * Vrátí počet záznamů odpovídajících daným kritériím.
     * @param array<string, mixed> $params
     * @throws DBException
     * @throws Throwable
     */
    public function count(string $tableName, array $params = [], ?callable $modifyParamsCallback = null): int
    {
        $params[self::PARAM_COUNT] = true;

        $result = $this->findBy($tableName, $params, $modifyParamsCallback);

        return is_int($result) ? $result : count($result);
    }


    /**
     * Chráněné, aby šlo odesílání dotazu obejít (testy, dekorátory nad klientem).
     * @throws DBException
     * @throws Throwable
     */
    protected function executeCount(
        string $tableName,
        FindByParams $params,
        ?callable $modifyParamsCallback,
    ): int
    {
        $request = $this->getRequestBuilder()->buildCountRequest($tableName, $params, $this->createWhereCompiler());

        try
        {
            $response = $this->responseToArray(
                $this->client->count($this->modifyRequest($request, $modifyParamsCallback)),
            );

            return (int) $response[self::PARAM_COUNT];
        }
        catch(Throwable $e)
        {
            $this->handleException($e);
        }
    }


    /**
     * Odešle dotaz do Elasticsearch a vrátí surovou odpověď.
     * @return mixed[]
     * @throws DBException
     * @throws Throwable
     */
    protected function executeSearch(
        string $tableName,
        FindByParams $params,
        ?callable $modifyParamsCallback,
    ): array
    {
        $request = $this->getRequestBuilder()->build($tableName, $params, $this->createWhereCompiler());

        try
        {
            return $this->responseToArray(
                $this->client->search($this->modifyRequest($request, $modifyParamsCallback)),
            );
        }
        catch(Throwable $e)
        {
            $this->handleException($e);
        }
    }


    /**
     * Celkový počet odpovídajících záznamů z odpovědi, pokud ho Elasticsearch uvedl.
     * @phpstan-ignore missingType.iterableValue
     */
    private static function totalHits(array $response): ?int
    {
        $total = $response['hits']['total']['value'] ?? null;

        return is_int($total) ? $total : null;
    }


    /**
     * Přeloží jednu where podmínku na Elasticsearch query.
     * Konverze hodnot jde přes convertToDBDataTypes(), aby ji šlo v potomcích přepsat.
     * @return Closure(string, mixed[]|null): array<string, mixed>
     */
    private function createWhereCompiler(): Closure
    {
        return fn (string $condition, ?array $values): array => $this->parseWhereCondition(
            $condition,
            $values !== null ? $this->convertToDBDataTypes($values) : null,
        );
    }


    /**
     * Dá volajícímu možnost sáhnout si na hotový dotaz před odesláním.
     * @param array<string, mixed> $request
     * @return array<string, mixed>
     */
    private function modifyRequest(array $request, ?callable $modifyParamsCallback): array
    {
        return $modifyParamsCallback !== null ? $modifyParamsCallback($request) : $request;
    }


    private function getRequestBuilder(): SearchRequestBuilder
    {
        return $this->requestBuilder ??= new SearchRequestBuilder(self::DEFAULT_LIMIT);
    }


    private function getResultMapper(): SearchResultMapper
    {
        return $this->resultMapper ??= new SearchResultMapper(self::PARAM_BUCKET, self::DEFAULT_GROUP_LIMIT);
    }


    private function getDataTypeConverter(): DataTypeConverter
    {
        return $this->dataTypeConverter ??= new DataTypeConverter();
    }


    public function convertToDBDataTypes(array $data): array
    {
        return $this->getDataTypeConverter()->toDatabase($data);
    }


    /**
     * @throws Throwable
     */
    public function convertFromDBDataTypes(array $data): array
    {
        return $this->getDataTypeConverter()->fromDatabase($data);
    }


    /**
     * @param array<string, array<string, array<mixed>>> $result
     */
    protected function addAndClause(array &$result, array $clause): void
    {
        $result['bool']['filter'][] = $clause;
    }


    /**
     * @param array<string, array<string, array<mixed>>> $result
     */
    protected function addOrClause(array &$result, array $clause): void
    {
        $result['bool']['should'][] = $clause;
    }


    /**
     * Operátory výrazů v pořadí, ve kterém se hledají. POŘADÍ JE VÝZNAMNÉ:
     *  - delší operátory musí být před svými předponami (`<=` před `<`),
     *  - `IN` je až úplně poslední, protože hodnoty LIKE a CROSS FIELDS jsou volný
     *    text a mohou samy obsahovat ` IN `; podmínka IN naopak žádný z operátorů
     *    nad sebou obsahovat nemůže.
     * @var list<array{string, string}> operátor -> metoda, která ho přeloží
     */
    private const EXPRESSION_OPERATORS = [
        ['<=', 'parseRangeExpression'],
        ['>=', 'parseRangeExpression'],
        ['!=', 'parseNotEqualExpression'],
        ['<', 'parseRangeExpression'],
        ['>', 'parseRangeExpression'],
        ['=', 'parseEqualExpression'],
        [' NOT LIKE ', 'parseNotLikeExpression'],
        [' LIKE ', 'parseLikeOperator'],
        [' IS NULL', 'parseIsNullExpression'],
        [' IS NOT NULL', 'parseIsNotNullExpression'],
        [' CROSS FIELDS ', 'parseCrossFieldsExpression'],
        [' NOT IN ', 'parseNotInExpression'],
        [' IN ', 'parseInExpression'],
    ];


    /** Elasticsearch klauzule pro jednotlivé porovnávací operátory. */
    private const RANGE_CLAUSES = [
        '<=' => 'lte',
        '>=' => 'gte',
        '<' => 'lt',
        '>' => 'gt',
    ];


    /**
     * @return array<string, mixed>
     * @throws DBException
     */
    protected function parseExpression(string $expr): array
    {
        foreach (self::EXPRESSION_OPERATORS as [$operator, $method])
        {
            $pos = mb_strpos($expr, $operator);

            if ($pos !== false)
            {
                return $this->{$method}($expr, $pos, $operator);
            }
        }

        throw new DBException('No expression matched.');
    }


    /**
     * @return array<string, mixed>
     */
    private function parseRangeExpression(string $expr, int $pos, string $operator): array
    {
        $result = [];
        $result['bool']['filter'][]['range'][self::fieldBefore($expr, $pos)][self::RANGE_CLAUSES[$operator]]
            = $this->normalizeValue(self::valueAfter($expr, $pos, $operator));

        return $result;
    }


    /**
     * @return array<string, mixed>
     */
    private function parseNotEqualExpression(string $expr, int $pos, string $operator): array
    {
        $result = [];
        $result['bool']['must_not']['match'][self::fieldBefore($expr, $pos)]
            = $this->normalizeValue(self::valueAfter($expr, $pos, $operator));

        return $result;
    }


    /**
     * @return array<string, mixed>
     */
    private function parseEqualExpression(string $expr, int $pos, string $operator): array
    {
        $value = self::valueAfter($expr, $pos, $operator);
        $field = self::fieldBefore($expr, $pos);
        $result = [];

        // Je možné zde dát pole, zadané takto [self::PARAM_WHERE]['sloupec = ?'] = [1, 2, 3];
        if (($listValue = $this->parseListValue($value)) !== null)
        {
            $result['terms'][$field] = $listValue;
        }
        else
        {
            $result['match'][$field] = $this->normalizeValue($value);
        }

        return $result;
    }


    /**
     * @return array<string, mixed>
     */
    private function parseNotLikeExpression(string $expr, int $pos, string $operator): array
    {
        return $this->parseLikeExpression(
            field: self::fieldBefore($expr, $pos),
            value: self::valueAfter($expr, $pos, $operator),
            negated: true,
        );
    }


    /**
     * @return array<string, mixed>
     */
    private function parseLikeOperator(string $expr, int $pos, string $operator): array
    {
        return $this->parseLikeExpression(
            field: self::fieldBefore($expr, $pos),
            value: self::valueAfter($expr, $pos, $operator),
        );
    }


    /**
     * @return array<string, mixed>
     */
    private function parseIsNullExpression(string $expr, int $pos, string $operator): array
    {
        $result = [];
        $result['bool']['must_not']['exists']['field'] = self::fieldBefore($expr, $pos);

        return $result;
    }


    /**
     * @return array<string, mixed>
     */
    private function parseIsNotNullExpression(string $expr, int $pos, string $operator): array
    {
        $result = [];
        $result['exists']['field'] = self::fieldBefore($expr, $pos);

        return $result;
    }


    /**
     * @return array<string, mixed>
     */
    private function parseCrossFieldsExpression(string $expr, int $pos, string $operator): array
    {
        return [
            'multi_match' => [
                'query' => self::valueAfter($expr, $pos, $operator),
                'type' => 'cross_fields',
                'operator' => 'and',
                'fields' => explode(',', self::fieldBefore($expr, $pos)),
            ],
        ];
    }


    /**
     * @return array<string, mixed>
     * @throws DBException
     */
    private function parseNotInExpression(string $expr, int $pos, string $operator): array
    {
        $result = [];
        $result['bool']['must_not']['terms'][self::fieldBefore($expr, $pos)]
            = $this->parseRequiredListValue(self::valueAfter($expr, $pos, $operator));

        return $result;
    }


    /**
     * @return array<string, mixed>
     * @throws DBException
     */
    private function parseInExpression(string $expr, int $pos, string $operator): array
    {
        $result = [];
        $result['terms'][self::fieldBefore($expr, $pos)]
            = $this->parseRequiredListValue(self::valueAfter($expr, $pos, $operator));

        return $result;
    }


    /**
     * Název sloupce, tedy část výrazu před operátorem.
     */
    private static function fieldBefore(string $expr, int $pos): string
    {
        return trim(mb_substr($expr, 0, $pos));
    }


    /**
     * Hodnota, tedy část výrazu za operátorem.
     */
    private static function valueAfter(string $expr, int $pos, string $operator): string
    {
        return trim(mb_substr($expr, $pos + mb_strlen($operator)));
    }


    /**
     * Rozparsuje where podmínku s hodnotami na Elasticsearch query.
     * Seznamy hodnot se do podmínky vkládají jako placeholdery, skutečné hodnoty
     * se předávají bokem (viz parseListValue).
     * @param mixed[]|null $values
     * @return array<string, mixed>
     * @throws DBException
     */
    protected function parseWhereCondition(string $condition, ?array $values): array
    {
        $this->listPlaceholders = [];

        return $this->parseBooleanQuery(
            $this->putValuesIntoQuery($condition, $values, placeholders: $this->listPlaceholders),
        );
    }


    /**
     * Rozparsuje seznam hodnot ve formátu `[a,b,c]`.
     * Vrací null, pokud hodnota není seznam.
     * @return list<int|string>|null
     */
    private function parseListValue(string $value): ?array
    {
        if (($start = strpos($value, '[')) === false || ($end = strpos($value, ']')) === false || $start >= $end)
        {
            return null;
        }

        $content = substr($value, $start + 1, $end - $start - 1);

        // Seznam předaný jako hodnota placeholderu - vracíme původní hodnoty beze změny.
        if (preg_match('/^#\d+#$/', $content) === 1 && isset($this->listPlaceholders[$content]))
        {
            return array_values($this->listPlaceholders[$content]);
        }

        $items = explode(',', $content);

        foreach ($items as $key => $item)
        {
            if ((int) $item == $item)
            {
                $items[$key] = (int) $item;
            }
        }

        return $items;
    }


    /**
     * Rozparsuje seznam hodnot ve formátu `[a,b,c]` (např. pro podmínku IN).
     * @return list<int|string>
     * @throws DBException
     */
    private function parseRequiredListValue(string $value): array
    {
        $listValue = $this->parseListValue($value);

        if ($listValue === null)
        {
            throw new DBException("Hodnota podmínky IN musí být seznam hodnot, zadáno: `$value`.");
        }

        return $listValue;
    }


    /**
     * Rozparsuje LIKE výraz (včetně volitelné klauzule ESCAPE) na wildcard query.
     * Escape znak se načítá z klauzule `ESCAPE 'x'`, pokud je definována.
     * @return array<string, mixed>
     */
    private function parseLikeExpression(string $field, string $value, bool $negated = false): array
    {
        // Volitelná klauzule ESCAPE - z ní se načte escape znak.
        $escapeChar = null;

        if (preg_match("/\s+ESCAPE\s+'(?<char>.)'$/u", $value, $matches) === 1)
        {
            $escapeChar = $matches['char'];
            $value = trim((string) preg_replace("/\s+ESCAPE\s+'.'$/u", '', $value));
        }

        // Hodnota může být SQL literál v uvozovkách (např. `col LIKE ''` nebo `col LIKE '%abc%'`).
        if (mb_strlen($value) >= 2 && str_starts_with($value, "'") && str_ends_with($value, "'"))
        {
            $value = mb_substr($value, 1, mb_strlen($value) - 2);
        }

        if ($this->wildcardValueFilter !== null)
        {
            $value = call_user_func($this->wildcardValueFilter, $value);
        }

        $wildcard = mb_strtolower($this->translateLikeToWildcard($value, $escapeChar));

        if ($negated)
        {
            $result['bool']['must_not']['wildcard'][$field] = $wildcard;
        }
        else
        {
            $result['wildcard'][$field] = $wildcard;
        }

        return $result;
    }


    /**
     * Převede SQL LIKE pattern na Elasticsearch wildcard pattern.
     * S definovaným escape znakem se uplatňuje plná SQL LIKE sémantika:
     * `%` -> `*`, `_` -> `?`, escapované znaky jsou literály a wildcard znaky
     * Elasticsearch (`*`, `?`, `\`) se v literálech escapují.
     * Bez escape znaku zůstává zachováno původní chování (pouze převod `%` na `*`).
     */
    private function translateLikeToWildcard(string $value, ?string $escapeChar): string
    {
        if ($escapeChar === null)
        {
            return (string) mb_ereg_replace('%', '*', $value);
        }

        $result = '';
        $chars = mb_str_split($value);
        $count = count($chars);

        for ($i = 0; $i < $count; $i++)
        {
            $char = $chars[$i];

            // Escapovaný znak je vždy literál.
            if ($char === $escapeChar && $i + 1 < $count)
            {
                $result .= $this->escapeWildcardCharacter($chars[++$i]);

                continue;
            }

            $result .= match ($char)
            {
                '%' => '*',
                '_' => '?',
                default => $this->escapeWildcardCharacter($char),
            };
        }

        return $result;
    }


    /**
     * Escapuje znak, který má ve wildcard pattern Elasticsearch speciální význam.
     */
    private function escapeWildcardCharacter(string $char): string
    {
        return in_array($char, ['*', '?', '\\'], true)
            ? '\\' . $char
            : $char;
    }


    /**
     * Normalizuje hodnotu parametru query.
     */
    private function normalizeValue(mixed $value): mixed
    {
        // Pokud se jedná o číslenou hodnotu, převedeme ji na int/float.
        if (
            is_numeric($value)
            // Pokud se jedná o celé číslo, které začíná nulou, není ho možné převést na číslo!
            && !(is_string($value) && ctype_digit($value) && $value[0] === '0')
        )
        {
            return ctype_digit($value) ? (int) $value : (float) $value;
        }

        return $value;
    }


    /**
     * Odchycení společných výjimek.
     * @throws DBException
     * @throws Throwable
     */
    private function handleException(Throwable $e): never
    {
        if ($this->getElasticErrorType($e) === 'index_not_found_exception')
        {
            throw new DBException(self::ERROR_DB_DOESNT_EXIST, $e->getCode());
        }

        throw $e;
    }


    /**
     * Vytáhne z výjimky Elasticsearch klienta typ chyby (error.type).
     * V Elasticsearch 8+ je tělo odpovědi součástí zprávy výjimky.
     */
    private function getElasticErrorType(Throwable $e): ?string
    {
        if (preg_match('/"type"\s*:\s*"([^"]+)"/', $e->getMessage(), $matches) === 1)
        {
            return $matches[1];
        }

        return null;
    }


    /**
     * Převede odpověď klienta na pole. Klient používáme synchronně, takže vrací vždy
     * Elasticsearch, nikdy Promise (ta se vrací jen v asynchronním režimu).
     *
     * Návratový typ je záměrně netypované `array` – jde o dynamickou JSON odpověď
     * z Elasticsearch a typování na `array<mixed>` by u navazujícího přístupu ke
     * vnořeným klíčům spouštělo na úrovni max chyby o přístupu na `mixed`.
     * @phpstan-ignore missingType.iterableValue
     */
    private function responseToArray(Elasticsearch|Promise $response): array
    {
        assert($response instanceof Elasticsearch);

        return $response->asArray();
    }
}
