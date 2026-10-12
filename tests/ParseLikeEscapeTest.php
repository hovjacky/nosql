<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Testy parsování LIKE podmínky s klauzulí ESCAPE.
 * Escape znak se načítá z klauzule ESCAPE 'x'. S definovaným escape znakem se uplatňuje
 * plná SQL LIKE sémantika: `%` -> `*`, `_` -> `?`, escapované znaky jsou literály
 * a wildcard znaky Elasticsearch (`*`, `?`, `\`) se v literálech escapují.
 */
final class ParseLikeEscapeTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    public function testLikeWithEscapeClauseBuildsWildcard(): void
    {
        self::assertSame(
            ['wildcard' => ['name' => '*abc*']],
            $this->client->buildQuery("name LIKE ? ESCAPE '~'", ['%abc%']),
        );
    }


    public function testEscapedPercentIsLiteral(): void
    {
        self::assertSame(
            ['wildcard' => ['discount' => '*50%*']],
            $this->client->buildQuery("discount LIKE ? ESCAPE '~'", ['%50~%%']),
        );
    }


    public function testEscapedUnderscoreIsLiteral(): void
    {
        self::assertSame(
            ['wildcard' => ['code' => '*a_b*']],
            $this->client->buildQuery("code LIKE ? ESCAPE '~'", ['%a~_b%']),
        );
    }


    public function testEscapedEscapeCharIsLiteral(): void
    {
        self::assertSame(
            ['wildcard' => ['code' => '*a~b*']],
            $this->client->buildQuery("code LIKE ? ESCAPE '~'", ['%a~~b%']),
        );
    }


    public function testUnescapedUnderscoreIsSingleCharWildcard(): void
    {
        self::assertSame(
            ['wildcard' => ['code' => '*a?c*']],
            $this->client->buildQuery("code LIKE ? ESCAPE '~'", ['%a_c%']),
        );
    }


    public function testElasticsearchWildcardCharactersAreEscaped(): void
    {
        // Hodnoty s `*` a `\` neprojdou přes sanitizaci placeholderů,
        // proto testujeme přímo podmínku bez placeholderu.
        self::assertSame(
            ['wildcard' => ['code' => '*a\\*b*']],
            $this->client->buildQuery("code LIKE %a*b% ESCAPE '~'"),
        );

        self::assertSame(
            ['wildcard' => ['code' => 'a\\\\b']],
            $this->client->buildQuery("code LIKE a\\b ESCAPE '~'"),
        );
    }


    public function testCustomEscapeCharacterIsReadFromEscapeClause(): void
    {
        self::assertSame(
            ['wildcard' => ['discount' => 'a%b']],
            $this->client->buildQuery("discount LIKE a!%b ESCAPE '!'"),
        );
    }


    public function testWildcardValueIsLowercased(): void
    {
        self::assertSame(
            ['wildcard' => ['name' => '*abc*']],
            $this->client->buildQuery("name LIKE ? ESCAPE '~'", ['%AbC%']),
        );
    }


    public function testTildeInValueSurvivesPlaceholderSanitization(): void
    {
        self::assertSame(
            ['match' => ['name' => 'a~b']],
            $this->client->buildQuery('name = ?', ['a~b']),
        );
    }
}
