<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query;

use DateTime;
use Hovjacky\NoSQL\ElasticsearchClient;
use Hovjacky\NoSQL\Query\SearchResultMapper;
use Hovjacky\NoSQL\Tests\TestableElasticsearchClient;
use PHPUnit\Framework\TestCase;

/**
 * Testy převodu odpovědi z Elasticsearch na řádky vracené z findBy().
 * Očekávané výsledky odpovídají implementaci před vytažením mapperu z findBy().
 */
final class SearchResultMapperTest extends TestCase
{
    private SearchResultMapper $mapper;

    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->mapper = new SearchResultMapper(
            ElasticsearchClient::PARAM_BUCKET,
            ElasticsearchClient::DEFAULT_GROUP_LIMIT,
        );
        $this->client = new TestableElasticsearchClient();
    }


    /**
     * @param array<string, mixed> $response
     * @param array<string, mixed> $params
     * @return array<int, array<string, mixed>>|int
     */
    private function map(array $response, array $params): array|int
    {
        return $this->mapper->map($response, $this->client->repairParams($params), $this->client->convertFromDBDataTypes(...));
    }


    /**
     * @param list<array{string, array<string, mixed>}> $rows
     * @return array<string, mixed>
     */
    private static function hits(array $rows): array
    {
        return ['hits' => ['hits' => array_map(
            static fn (array $row): array => ['_id' => $row[0], '_source' => $row[1]],
            $rows,
        )]];
    }


    public function testPlainHitsReturnSources(): void
    {
        self::assertSame(
            [['name' => 'Jan', 'age' => 30], ['name' => 'Petr', 'age' => 40]],
            $this->map(self::hits([['1', ['name' => 'Jan', 'age' => 30]], ['2', ['name' => 'Petr', 'age' => 40]]]), []),
        );
    }


    public function testEmptyHitsReturnEmptyArray(): void
    {
        self::assertSame([], $this->map(self::hits([]), []));
    }


    public function testIdIsPrependedWhenRequestedInFields(): void
    {
        self::assertSame(
            [['id' => '7', 'name' => 'Jan']],
            $this->map(self::hits([['7', ['name' => 'Jan']]]), ['fields' => ['id', 'name']]),
        );
    }


    public function testIdIsNotAddedWhenNotRequested(): void
    {
        self::assertSame(
            [['name' => 'Jan']],
            $this->map(self::hits([['7', ['name' => 'Jan']]]), ['fields' => ['name']]),
        );
    }


    public function testDateLikeStringsBecomeDateTime(): void
    {
        $result = $this->map(
            self::hits([['1', ['created' => '2024-01-31', 'at' => '2024-01-31T12:30', 'txt' => 'ahoj']]]),
            [],
        );

        self::assertIsArray($result);
        self::assertInstanceOf(DateTime::class, $result[0]['created']);
        self::assertInstanceOf(DateTime::class, $result[0]['at']);
        self::assertSame('ahoj', $result[0]['txt']);
    }


    public function testNestedValuesAreConvertedRecursively(): void
    {
        $result = $this->map(self::hits([['1', ['sub' => ['d' => '2024-01-31', 'n' => 5]]]]), []);

        self::assertIsArray($result);
        self::assertInstanceOf(DateTime::class, $result[0]['sub']['d']);
        self::assertSame(5, $result[0]['sub']['n']);
    }


    public function testAggregationsWithoutGroupByReturnSingleRow(): void
    {
        self::assertSame(
            [['min_id' => 1, 'min_age' => 18, 'avg_age' => 33.5]],
            $this->map(
                ['aggregations' => ['min_id' => ['value' => 1], 'min_age' => ['value' => 18], 'avg_age' => ['value' => 33.5]]],
                ['aggregation' => ['min' => ['id', 'age'], 'avg' => ['age']]],
            ),
        );
    }


    public function testScalarAggregationColumnIsAccepted(): void
    {
        self::assertSame(
            [['max_age' => 99]],
            $this->map(['aggregations' => ['max_age' => ['value' => 99]]], ['aggregation' => ['max' => 'age']]),
        );
    }


    public function testBucketsWithoutTopHitsReturnKeyAndCount(): void
    {
        $result = $this->map(
            ['aggregations' => ['group_by' => ['buckets' => [
                ['key' => 'Jan', 'doc_count' => 3],
                ['key' => 'Petr', 'doc_count' => 1],
            ]]]],
            ['groupBy' => 'name'],
        );

        self::assertIsArray($result);
        self::assertCount(2, $result);
        self::assertSame('Jan', $result[0]['name']);
        self::assertSame(3, $result[0]['count']);
        self::assertSame(['key' => 'Jan', 'doc_count' => 3], $result[0][ElasticsearchClient::PARAM_BUCKET]);
    }


    public function testBucketsWithTopHitsReturnSource(): void
    {
        $result = $this->map(
            ['aggregations' => ['group_by' => ['buckets' => [[
                'key' => 'Jan',
                'doc_count' => 3,
                'results' => ['hits' => ['hits' => [['_source' => ['name' => 'Jan', 'age' => 30]]]]],
            ]]]]],
            ['groupBy' => 'name', 'orderBy' => ['age']],
        );

        self::assertIsArray($result);
        self::assertSame('Jan', $result[0]['name']);
        self::assertSame(30, $result[0]['age']);
    }


    public function testBucketAggregationValuesAreAdded(): void
    {
        $result = $this->map(
            ['aggregations' => ['group_by' => ['buckets' => [
                ['key' => 'Jan', 'doc_count' => 3, 'max_age' => ['value' => 44]],
            ]]]],
            ['groupBy' => 'name', 'aggregation' => ['max' => ['age']]],
        );

        self::assertIsArray($result);
        self::assertSame(44, $result[0]['max_age']);
    }


    public function testOffsetSlicesBuckets(): void
    {
        $result = $this->map(
            ['aggregations' => ['group_by' => ['buckets' => [
                ['key' => 'a', 'doc_count' => 1],
                ['key' => 'b', 'doc_count' => 2],
                ['key' => 'c', 'doc_count' => 3],
            ]]]],
            ['groupBy' => 'name', 'offset' => 1],
        );

        self::assertIsArray($result);
        self::assertCount(2, $result);
        self::assertSame('b', $result[0]['name']);
    }


    public function testCountWithGroupByReturnsNumberOfBuckets(): void
    {
        self::assertSame(
            2,
            $this->map(
                ['aggregations' => ['group_by' => ['buckets' => [
                    ['key' => 'a', 'doc_count' => 1],
                    ['key' => 'b', 'doc_count' => 2],
                ]]]],
                ['groupBy' => 'name', 'count' => true],
            ),
        );
    }
}
