<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Testy parsování LIKE podmínky se SQL literálem v uvozovkách (např. `col LIKE ''`).
 */
final class ParseLikeLiteralTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    public function testLikeEmptyStringLiteral(): void
    {
        self::assertSame(
            ['wildcard' => ['name' => '']],
            $this->client->buildQuery("name LIKE ''"),
        );
    }


    public function testNotLikeEmptyStringLiteral(): void
    {
        self::assertSame(
            ['bool' => ['must_not' => ['wildcard' => ['name' => '']]]],
            $this->client->buildQuery("name NOT LIKE ''"),
        );
    }


    public function testLikeQuotedLiteral(): void
    {
        self::assertSame(
            ['wildcard' => ['name' => '*abc*']],
            $this->client->buildQuery("name LIKE '%abc%'"),
        );
    }


    public function testLikeValueContainingInKeywordIsParsedAsLike(): void
    {
        self::assertSame(
            ['wildcard' => ['name' => '*check in*']],
            $this->client->buildQuery("name LIKE ? ESCAPE '~'", ['%CHECK IN%']),
        );
    }
}
