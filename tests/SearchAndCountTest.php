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
    private static function hits(array $rows, ?int $total = null, string $relation = 'eq'): array
    {
        $response = ['hits' => ['hits' => array_map(
            static fn (array $row): array => ['_id' => $row[0], '_source' => $row[1]],
            $rows,
        )]];

        if ($total !== null)
        {
            $response['hits']['total'] = ['value' => $total, 'relation' => $relation];
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


    /**
     * Elasticsearch ve výchozím nastavení přestává počítat na 10 000 shodách a dál hlásí
     * jen `10000, gte`. Stránkovadlo postavené na takovém čísle by ukazovalo nesmysl,
     * proto musí jít poznat, že je číslo jen dolní mez.
     */
    public function testSearchSaysWhetherTotalIsExact(): void
    {
        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']]], 42);

        self::assertTrue($this->client->search('lidi', [])->isTotalExact());

        $this->client->fakeResponse = self::hits([['1', ['name' => 'Jan']]], 10000, 'gte');

        $result = $this->client->search('lidi', []);

        self::assertSame(10000, $result->total);
        self::assertSame('gte', $result->totalRelation);
        self::assertFalse($result->isTotalExact());
    }


    /**
     * Odpověď se testům podstrčí až na hranici odesílání, takže se dotaz opravdu sestaví.
     */
    public function testSearchSendsTheBuiltRequest(): void
    {
        $this->client->fakeResponse = self::hits([]);

        $this->client->search('lidi', ['where' => ['age > ?' => 18], 'limit' => 10, 'offset' => 5]);

        self::assertSame(
            [
                'index' => 'lidi',
                'body' => [
                    'query' => ['bool' => ['filter' => [['range' => ['age' => ['gt' => 18]]]]]],
                    'size' => 10,
                    'from' => 5,
                ],
            ],
            $this->client->lastRequest,
        );
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
     * S GROUP BY se počet skupin zjišťuje agregací `cardinality` nad běžným vyhledáváním,
     * protože `terms` vrátí jen tolik bucketů, kolik se jich vyžádalo.
     */
    public function testCountWithGroupByAsksForCardinalityOfGroups(): void
    {
        $this->client->fakeResponse = ['aggregations' => ['group_count' => ['value' => 250000]]];

        self::assertSame(250000, $this->client->count('lidi', ['groupBy' => 'name']));

        self::assertSame(
            [
                'size' => 0,
                'aggs' => ['group_count' => ['cardinality' => ['field' => 'name', 'precision_threshold' => 40000]]],
            ],
            $this->client->lastRequest['body'] ?? null,
        );
    }


    /**
     * Dotaz na `_count` snese v těle jen `query`; `from` ani `aggs` by Elasticsearch odmítl.
     */
    public function testCountRequestContainsNothingButTheQuery(): void
    {
        $this->client->count('lidi', [
            'where' => ['age > ?' => 18],
            'limit' => 10,
            'offset' => 5,
            'fields' => ['id'],
            'aggregation' => ['max' => ['age']],
        ]);

        self::assertSame(
            ['index' => 'lidi', 'body' => ['query' => ['bool' => ['filter' => [['range' => ['age' => ['gt' => 18]]]]]]]],
            $this->client->lastRequest,
        );
    }


    public function testCountRequestWithoutWhereHasEmptyBody(): void
    {
        $this->client->count('lidi', ['limit' => 10, 'offset' => 5]);

        self::assertSame(['index' => 'lidi', 'body' => []], $this->client->lastRequest);
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
