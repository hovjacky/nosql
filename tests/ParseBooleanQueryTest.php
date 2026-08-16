<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Charakterizační testy stávajícího parsování where podmínek na Elasticsearch query.
 */
final class ParseBooleanQueryTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    public function testEquals(): void
    {
        self::assertSame(
            ['match' => ['name' => 'John']],
            $this->client->buildQuery('name = ?', ['John']),
        );
    }


    public function testEqualsNormalizesIntValue(): void
    {
        self::assertSame(
            ['match' => ['age' => 30]],
            $this->client->buildQuery('age = ?', [30]),
        );
    }


    public function testNotEquals(): void
    {
        self::assertSame(
            ['bool' => ['must_not' => ['match' => ['age' => 30]]]],
            $this->client->buildQuery('age != ?', [30]),
        );
    }


    public function testRangeOperators(): void
    {
        self::assertSame(
            ['bool' => ['filter' => [['range' => ['age' => ['lte' => 30]]]]]],
            $this->client->buildQuery('age <= ?', [30]),
        );

        self::assertSame(
            ['bool' => ['filter' => [['range' => ['age' => ['gte' => 30]]]]]],
            $this->client->buildQuery('age >= ?', [30]),
        );

        self::assertSame(
            ['bool' => ['filter' => [['range' => ['age' => ['lt' => 30]]]]]],
            $this->client->buildQuery('age < ?', [30]),
        );

        self::assertSame(
            ['bool' => ['filter' => [['range' => ['age' => ['gt' => 30]]]]]],
            $this->client->buildQuery('age > ?', [30]),
        );
    }


    public function testEqualsWithArrayValueBuildsTerms(): void
    {
        self::assertSame(
            ['terms' => ['id' => [1, 2, 3]]],
            $this->client->buildQuery('id = ?', [[1, 2, 3]]),
        );
    }


    public function testLikeBuildsLowercasedWildcard(): void
    {
        self::assertSame(
            ['wildcard' => ['name' => '*john*']],
            $this->client->buildQuery('name LIKE ?', ['%JoHn%']),
        );
    }


    public function testIsNull(): void
    {
        self::assertSame(
            ['bool' => ['must_not' => ['exists' => ['field' => 'name']]]],
            $this->client->buildQuery('name IS NULL'),
        );
    }


    public function testIsNotNull(): void
    {
        self::assertSame(
            ['exists' => ['field' => 'name']],
            $this->client->buildQuery('name IS NOT NULL'),
        );
    }


    public function testAndCombinesToBoolFilter(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'filter' => [
                        ['match' => ['name' => 'John']],
                        ['match' => ['age' => 30]],
                    ],
                ],
            ],
            $this->client->buildQuery('name = ? AND age = ?', ['John', 30]),
        );
    }


    public function testOrCombinesToBoolShould(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'should' => [
                        ['bool' => ['filter' => [['match' => ['name' => 'John']]]]],
                        ['bool' => ['filter' => [['match' => ['age' => 30]]]]],
                    ],
                ],
            ],
            $this->client->buildQuery('name = ? OR age = ?', ['John', 30]),
        );
    }


    public function testParenthesesGrouping(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'filter' => [
                        ['match' => ['name' => 'John']],
                        [
                            'bool' => [
                                'should' => [
                                    ['bool' => ['filter' => [['match' => ['age' => 30]]]]],
                                    ['bool' => ['filter' => [['match' => ['city' => 'Praha']]]]],
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            $this->client->buildQuery('name = ? AND (age = ? OR city = ?)', ['John', 30, 'Praha']),
        );
    }


    public function testValueSanitizationStripsForbiddenCharacters(): void
    {
        self::assertSame(
            ['match' => ['name' => 'John']],
            $this->client->buildQuery('name = ?', ['J(o)h<n>=\'']),
        );
    }
}
