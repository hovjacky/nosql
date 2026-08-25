<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use DateTime;
use Hovjacky\NoSQL\Query\SearchResult;
use PHPUnit\Framework\TestCase;

/**
 * Testy typovaného API search() / count() a jeho vztahu k původní findBy().
 */
final class SearchAndCountTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    /**
     * @param list<array{string, array<string, mixed>}> $rows
     * @return array<string, mixed>
     */
    private static function hits(array $rows, ?int $total = null): array
    {
        $response = ['hits' => ['hits' => array_map(
            static fn (array $row): array => ['_id' => $row[0], '_source' => $row[1]],
            $rows,
        )]];

        if ($total !== null)
        {
            $response['hits']['total'] = ['value' => $total, 'relation' => 'eq'];
        }

        return $response;
    }


    public function testSearchReturnsRowsAndRawResponse(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']], ['2', ['name' => 'Petr']]], 42);

        $result = $this->client->search('lidi', []);

        self::assertInstanceOf(SearchResult::class, $result);
        self::assertSame([['name' => 'Jan'], ['name' => 'Petr']], $result->rows);
        self::assertSame(42, $result->total);
        self::assertSame($this->client->fakeResponse, $result->response);
        self::assertSame(2, $result->countRows());
    }


    public function testSearchTotalIsNullWhenElasticsearchDoesNotReportIt(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']]]);

        self::assertNull($this->client->search('lidi', [])->total);
    }


    public function testSearchConvertsDataTypes(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['created' => '2024-01-31']]]);

        $row = $this->client->search('lidi', [])->first();

        self::assertNotNull($row);
        self::assertInstanceOf(DateTime::class, $row['created']);
    }


    /**
     * search() vrací vždy záznamy, i když volající omylem předá `count`.
     */
    public function testSearchIgnoresCountParameter(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']]], 1);

        $result = $this->client->search('lidi', ['count' => true]);

        self::assertSame([['name' => 'Jan']], $result->rows);
    }


    public function testSearchIsIterable(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']], ['2', ['name' => 'Petr']]]);

        $names = [];

        foreach ($this->client->search('lidi', []) as $row)
        {
            $names[] = $row['name'];
        }

        self::assertSame(['Jan', 'Petr'], $names);
    }


    public function testCountUsesDedicatedEndpointWithoutGroupBy(): void
    {
        $this->client->fakeCount = 123;

        self::assertSame(123, $this->client->count('lidi', ['where' => ['age > ?' => 18]]));
    }


    public function testCountWithoutParams(): void
    {
        $this->client->fakeCount = 7;

        self::assertSame(7, $this->client->count('lidi'));
    }


    /**
     * S GROUP BY se počítají buckets, tedy se prochází běžná odpověď vyhledávání.
     */
    public function testCountWithGroupByCountsBuckets(): void
    {
        $this->client->fakeResponse = ['aggregations' => ['group_by' => ['buckets' => [
            ['key' => 'a', 'doc_count' => 1],
            ['key' => 'b', 'doc_count' => 2],
            ['key' => 'c', 'doc_count' => 3],
        ]]]];

        self::assertSame(3, $this->client->count('lidi', ['groupBy' => 'name']));
    }


    /**
     * findBy() zůstává funkční včetně referenčního parametru se surovou odpovědí.
     */
    public function testFindByStillFillsDeprecatedResultDataParameter(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']]], 5);

        $rows = $this->client->findBy('lidi', [], null, $resultData);

        self::assertSame([['name' => 'Jan']], $rows);
        self::assertSame($this->client->fakeResponse, $resultData);
    }


    public function testFindByStillReturnsIntForCount(): void
    {
        $this->client->fakeCount = 9;

        self::assertSame(9, $this->client->findBy('lidi', ['count' => true]));
    }


    public function testSearchAndFindByAgreeOnRows(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']], ['2', ['name' => 'Petr']]], 2);

        self::assertSame($this->client->findBy('lidi', []), $this->client->search('lidi', [])->rows);
    }
}
