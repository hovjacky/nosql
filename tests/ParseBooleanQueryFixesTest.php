<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DBException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Testy chování, které se opravilo přepsáním parseru where podmínek.
 * Původní parser skládal dotaz řezáním řetězce a v těchto případech selhával.
 */
final class ParseBooleanQueryFixesTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    /**
     * Dřív se závorková skupina za dvěma a více výrazy zahodila a hodnota posledního
     * výrazu se poškodila (`b` se hledalo jako text `2 AND`).
     */
    public function testGroupAfterSeveralConditionsIsNotDropped(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'filter' => [
                        ['match' => ['a' => 1]],
                        ['match' => ['b' => 2]],
                        [
                            'bool' => [
                                'should' => [
                                    ['bool' => ['filter' => [['match' => ['c' => 3]]]]],
                                    ['bool' => ['filter' => [['match' => ['d' => 4]]]]],
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            $this->client->buildQuery('a = ? AND b = ? AND (c = ? OR d = ?)', [1, 2, 3, 4]),
        );
    }


    public function testGroupBeforeSeveralConditionsKeepsAllOfThem(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'filter' => [
                        [
                            'bool' => [
                                'should' => [
                                    ['bool' => ['filter' => [['match' => ['a' => 1]]]]],
                                    ['bool' => ['filter' => [['match' => ['b' => 2]]]]],
                                ],
                            ],
                        ],
                        ['match' => ['c' => 3]],
                        ['match' => ['d' => 4]],
                    ],
                ],
            ],
            $this->client->buildQuery('(a = ? OR b = ?) AND c = ? AND d = ?', [1, 2, 3, 4]),
        );
    }


    /**
     * @return iterable<string, array{string, array<string, mixed>}>
     */
    public static function literalProvider(): iterable
    {
        yield 'AND v literálu' => ["name LIKE '%a AND b%'", ['wildcard' => ['name' => '*a and b*']]];
        yield 'OR v literálu' => ["name LIKE '%a OR b%'", ['wildcard' => ['name' => '*a or b*']]];
        yield 'závorky v literálu' => ["name LIKE '%a (b)%'", ['wildcard' => ['name' => '*a (b)*']]];
    }


    /**
     * Text v apostrofech je pro parser jeden kus, spojky ani závorky v něm dotaz nerozbijí.
     * @param array<string, mixed> $expected
     */
    #[DataProvider('literalProvider')]
    public function testKeywordsInsideLiteralDoNotSplitTheQuery(string $query, array $expected): void
    {
        self::assertSame($expected, $this->client->buildQuery($query));
    }


    public function testLiteralWithKeywordCombinesWithOtherConditions(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'filter' => [
                        ['wildcard' => ['name' => '*a and b*']],
                        ['match' => ['age' => 30]],
                    ],
                ],
            ],
            $this->client->buildQuery("name LIKE '%a AND b%' AND age = ?", [30]),
        );
    }


    /**
     * @return iterable<string, array{string}>
     */
    public static function malformedProvider(): iterable
    {
        yield 'AND na konci' => ['a = 1 AND'];
        yield 'OR na konci' => ['a = 1 OR'];
        yield 'AND na začátku' => ['AND a = 1'];
        yield 'OR na začátku' => ['OR a = 1'];
        yield 'prázdné závorky' => ['()'];
    }


    /**
     * Neúplná podmínka dřív tiše vytvořila nesmyslný dotaz (`{"match":{"AND a":1}}`),
     * případně spadla na TypeError. Teď se hlásí jako DBException.
     */
    #[DataProvider('malformedProvider')]
    public function testMalformedConditionThrowsInsteadOfBuildingNonsense(string $query): void
    {
        $this->expectException(DBException::class);

        $this->client->buildQuery($query);
    }


    /**
     * Klíčové slovo musí stát samostatně - `ANDROID` ani `BRAND` nesmí dotaz rozdělit.
     */
    public function testKeywordInsideWordDoesNotSplitTheQuery(): void
    {
        self::assertSame(['match' => ['os' => 'ANDROID']], $this->client->buildQuery('os = ?', ['ANDROID']));
        self::assertSame(['match' => ['BRAND' => 'x']], $this->client->buildQuery('BRAND = ?', ['x']));
    }
}
