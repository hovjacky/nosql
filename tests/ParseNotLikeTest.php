<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Testy parsování negované LIKE podmínky (NOT LIKE).
 */
final class ParseNotLikeTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    public function testNotLikeBuildsMustNotWildcard(): void
    {
        self::assertSame(
            ['bool' => ['must_not' => ['wildcard' => ['name' => '*john*']]]],
            $this->client->buildQuery('name NOT LIKE ?', ['%John%']),
        );
    }


    public function testNotLikeWithEscapeClause(): void
    {
        self::assertSame(
            ['bool' => ['must_not' => ['wildcard' => ['discount' => '*50%*']]]],
            $this->client->buildQuery("discount NOT LIKE ? ESCAPE '~'", ['%50~%%']),
        );
    }


    public function testNotLikeCombinedWithIsNull(): void
    {
        self::assertSame(
            [
                'bool' => [
                    'should' => [
                        ['bool' => ['filter' => [['bool' => ['must_not' => ['wildcard' => ['name' => '*john*']]]]]]],
                        ['bool' => ['filter' => [['bool' => ['must_not' => ['exists' => ['field' => 'name']]]]]]],
                    ],
                ],
            ],
            $this->client->buildQuery("name NOT LIKE ? ESCAPE '~' OR name IS NULL", ['%john%']),
        );
    }
}
