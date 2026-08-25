<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query;

use DateTime;
use DateTimeZone;
use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use Hovjacky\NoSQL\ElasticsearchClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

/**
 * Charakterizační testy sestavení Elasticsearch dotazu.
 *
 * Očekávané dotazy v tests/fixtures/find_by_requests.json byly pořízeny z implementace
 * před vytažením SearchRequestBuilderu, takže hlídají, že se chování nezměnilo.
 */
final class SearchRequestBuilderTest extends TestCase
{
    private ElasticsearchClient $client;


    protected function setUp(): void
    {
        // Ke spojení nedojde - dotaz odchytíme v callbacku dřív, než se odešle.
        $this->client = new ElasticsearchClient(['host' => '127.0.0.1', 'port' => 9]);
    }


    /**
     * @return array<string, array<string, mixed>>
     */
    public static function cases(): array
    {
        $date = new DateTime('2024-01-31 12:00:00', new DateTimeZone('UTC'));

        return [
            'plain' => [],
            'fields' => ['fields' => ['id', 'name']],
            'limit' => ['limit' => 10],
            'limit_offset' => ['limit' => 10, 'offset' => 5],
            'offset_only' => ['offset' => 5],
            'count' => ['count' => true],
            'count_limit' => ['count' => true, 'limit' => 10],
            'order_single' => ['orderBy' => ['id']],
            'order_desc' => ['orderBy' => ['id desc', 'name']],
            'order_scalar' => ['orderBy' => 'id desc'],
            'order_with_count' => ['orderBy' => ['id'], 'count' => true],
            'group' => ['groupBy' => 'name'],
            'group_limit' => ['groupBy' => 'name', 'limit' => 10],
            'group_limit_offset' => ['groupBy' => 'name', 'limit' => 10, 'offset' => 3],
            'group_order' => ['groupBy' => 'name', 'orderBy' => ['name desc']],
            'group_order_count' => ['groupBy' => 'name', 'orderBy' => ['count desc']],
            'group_order_other' => ['groupBy' => 'name', 'orderBy' => ['age desc', 'id']],
            'group_order_fields' => ['groupBy' => 'name', 'orderBy' => ['age'], 'fields' => ['id', 'age']],
            'group_internal' => ['groupBy' => 'name', 'orderBy' => ['age'], 'groupInternalOrderBy' => ['id desc']],
            'group_internal_scalar' => ['groupBy' => 'name', 'orderBy' => ['age'], 'groupInternalOrderBy' => 'id desc'],
            'group_script' => ['groupBy' => 'script', 'groupByScript' => "doc['a'].value"],
            'group_script_noscript' => ['groupBy' => 'script'],
            'agg' => ['aggregation' => ['min' => ['id', 'age'], 'avg' => ['age']]],
            'agg_scalar' => ['aggregation' => ['max' => 'age']],
            'agg_group' => ['groupBy' => 'name', 'aggregation' => ['min' => ['age']]],
            'agg_order' => ['aggregation' => ['min' => ['age']], 'orderBy' => ['id']],
            'where_one' => ['where' => ['id = ?' => 5]],
            'where_two' => ['where' => ['id = ?' => 5, 'name LIKE ?' => 'Jan%']],
            'where_list' => ['where' => ['id = ?' => [[1, 2, 3]]]],
            'where_in' => ['where' => ['id IN ?' => [[1, 2, 3]]]],
            'where_date' => ['where' => ['created >= ?' => $date]],
            'where_bool' => ['where' => ['(a = ? AND b = ?) OR c = ?' => [1, 2, 3]]],
            'where_isnull' => ['where' => ['name IS NULL' => null]],
            'where_count' => ['where' => ['id = ?' => 5], 'count' => true],
            'where_group_agg' => [
                'where' => ['id > ?' => 1],
                'groupBy' => 'name',
                'orderBy' => ['age desc'],
                'aggregation' => ['max' => ['age']],
                'limit' => 7,
                'offset' => 2,
            ],

            // Hraniční hodnoty - hlídají sémantiku empty(), na které stojí volitelnost parametrů.
            'limit_zero' => ['limit' => 0],
            'limit_string' => ['limit' => '10'],
            'limit_string_offset' => ['limit' => '10', 'offset' => '5'],
            'offset_zero' => ['limit' => 10, 'offset' => 0],
            'offset_nonnumeric' => ['limit' => 10, 'offset' => 'abc'],
            'count_false' => ['count' => false],
            'count_zero' => ['count' => 0],
            'count_string' => ['count' => '1'],
            'fields_empty' => ['fields' => []],
            'fields_zero' => ['fields' => 0],
            'where_empty' => ['where' => []],
            'orderby_empty' => ['orderBy' => []],
            'orderby_empty_string' => ['orderBy' => ''],
            'group_empty_string' => ['groupBy' => ''],
            'group_zero' => ['groupBy' => 0],
            'agg_empty' => ['aggregation' => []],
            'agg_zero' => ['aggregation' => 0],
            'group_script_empty' => ['groupBy' => 'script', 'groupByScript' => ''],
            'extra_unknown_key' => ['limit' => 3, 'somethingCustom' => 'x'],
            'group_limit_str_off' => ['groupBy' => 'n', 'limit' => '7', 'offset' => '2'],
            'group_internal_nocnt' => ['groupBy' => 'n', 'orderBy' => ['a'], 'groupInternalOrderBy' => ['b desc'], 'count' => true],
            'order_desc_midword' => ['orderBy' => ['my desc column']],
            'agg_int_column' => ['aggregation' => ['min' => [5]]],
            'where_scalar_wrapped' => ['where' => ['id = ?' => 5]],
        ];
    }


    /**
     * @return iterable<string, array{array<string, mixed>, string}>
     */
    public static function invalidParamsProvider(): iterable
    {
        yield 'fields nejsou pole' => [['fields' => 'id'], DB::ERROR_FIELDS];
        yield 'where není pole' => [['where' => 'x'], DB::ERROR_WHERE];
        yield 'where má číselný klíč' => [['where' => [0 => 'x']], DB::ERROR_WHERE];
        yield 'aggregation není pole' => [['aggregation' => 'x'], DB::ERROR_AGGREGATION];
    }


    /**
     * @param array<string, mixed> $params
     */
    #[DataProvider('invalidParamsProvider')]
    public function testInvalidParamsAreRejected(array $params, string $expectedMessage): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage($expectedMessage);

        $this->client->findBy('mytable', $params, static fn (array $request): never => throw new CapturedRequest($request));
    }


    /**
     * @return iterable<string, array{string, array<string, mixed>}>
     */
    public static function caseProvider(): iterable
    {
        foreach (self::cases() as $name => $params)
        {
            yield $name => [$name, $params];
        }
    }


    /**
     * @param array<string, mixed> $params
     */
    #[DataProvider('caseProvider')]
    public function testBuiltRequestMatchesFixture(string $name, array $params): void
    {
        self::assertSame(self::expectedRequests()[$name], $this->capture($params), "Dotaz pro případ `{$name}`");
    }


    public function testFixtureCoversEveryCase(): void
    {
        self::assertSame(array_keys(self::cases()), array_keys(self::expectedRequests()));
    }


    /**
     * `IS NULL` podmínka bez hodnot dřív spadla na TypeError, než se stihl dotaz sestavit.
     */
    public function testNullWhereValueBuildsQueryInsteadOfFailing(): void
    {
        $request = $this->capture(['where' => ['name IS NULL' => null]]);

        self::assertSame(['bool' => ['must_not' => ['exists' => ['field' => 'name']]]], $request['body']['query']);
    }


    /**
     * @param array<string, mixed> $params
     * @return array<string, mixed>
     */
    private function capture(array $params): array
    {
        try
        {
            $this->client->findBy('mytable', $params, static function (array $request): never {
                throw new CapturedRequest($request);
            });
        }
        catch(CapturedRequest $e)
        {
            return $e->request;
        }
        catch(Throwable $e)
        {
            self::fail('Dotaz se nepodařilo sestavit: ' . $e::class . ': ' . $e->getMessage());
        }

        self::fail('Callback nebyl zavolán, dotaz se neodchytil.');
    }


    /**
     * @return array<string, array<string, mixed>>
     */
    private static function expectedRequests(): array
    {
        $json = file_get_contents(__DIR__ . '/../fixtures/find_by_requests.json');

        self::assertIsString($json);

        $decoded = json_decode($json, true);

        self::assertIsArray($decoded);

        return $decoded;
    }
}
