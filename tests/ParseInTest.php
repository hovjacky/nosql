<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DBException;
use PHPUnit\Framework\TestCase;

/**
 * Testy parsování IN / NOT IN podmínky.
 * Seznam hodnot se předává jako pole bindnuté na jeden placeholder: `col IN ?` s hodnotou [1, 2, 3].
 */
final class ParseInTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    public function testInBuildsTerms(): void
    {
        self::assertSame(
            ['terms' => ['id' => [1, 2, 3]]],
            $this->client->buildQuery('id IN ?', [[1, 2, 3]]),
        );
    }


    public function testInWithStringValues(): void
    {
        self::assertSame(
            ['terms' => ['code' => ['abc', 'def']]],
            $this->client->buildQuery('code IN ?', [['abc', 'def']]),
        );
    }


    public function testNotInBuildsMustNotTerms(): void
    {
        self::assertSame(
            ['bool' => ['must_not' => ['terms' => ['id' => [1, 2, 3]]]]],
            $this->client->buildQuery('id NOT IN ?', [[1, 2, 3]]),
        );
    }


    public function testNotInCombinedWithIsNull(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'should' => [
                        ['bool' => ['filter' => [['bool' => ['must_not' => ['terms' => ['id' => [1, 2]]]]]]]],
                        ['bool' => ['filter' => [['bool' => ['must_not' => ['exists' => ['field' => 'id']]]]]]],
                    ],
                ],
            ],
            $this->client->buildQuery('id NOT IN ? OR id IS NULL', [[1, 2]]),
        );
    }


    public function testInValuesContainingCommasAreParsedReliably(): void
    {
        self::assertSame(
            ['terms' => ['name' => ['Novák, Jan', 'Svoboda, Petr']]],
            $this->client->buildQuery('name IN ?', [['Novák, Jan', 'Svoboda, Petr']]),
        );
    }


    public function testInValuesKeepOriginalTypes(): void
    {
        self::assertSame(
            ['terms' => ['code' => [1, '02', 'abc']]],
            $this->client->buildQuery('code IN ?', [[1, '02', 'abc']]),
        );
    }


    public function testEqualsWithArrayValueContainingCommasBuildsExactTerms(): void
    {
        self::assertSame(
            ['terms' => ['name' => ['Novák, Jan', 'Svoboda, Petr']]],
            $this->client->buildQuery('name = ?', [['Novák, Jan', 'Svoboda, Petr']]),
        );
    }


    public function testCrossFieldsValueContainingInKeywordIsParsedAsCrossFields(): void
    {
        self::assertSame(
            [
                'multi_match' => [
                    'query' => 'CHECK IN HOTEL',
                    'type' => 'cross_fields',
                    'operator' => 'and',
                    'fields' => ['name', 'description'],
                ],
            ],
            $this->client->buildQuery('name,description CROSS FIELDS CHECK IN HOTEL'),
        );
    }


    public function testInWithoutListValueThrows(): void
    {
        $this->expectException(DBException::class);

        $this->client->buildQuery('id IN ?', [5]);
    }
}
