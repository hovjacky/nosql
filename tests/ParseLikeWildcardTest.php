<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Testy převodu LIKE vzoru na wildcard pattern bez klauzule ESCAPE.
 *
 * Zástupným znakem je `%`, tak to má SQL LIKE. Wildcard znaky Elasticsearch
 * (`*`, `?`, `\`) zástupné nejsou, takže se z hodnoty escapují.
 */
final class ParseLikeWildcardTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    /**
     * @return iterable<string, array{string, string}>
     */
    public static function valueProvider(): iterable
    {
        yield 'procento je zástupný znak' => ['%abc%', '*abc*'];
        yield 'hvězdička je literál' => ['a*b', 'a\\*b'];
        yield 'otazník je literál' => ['a?b', 'a\\?b'];
        yield 'zpětné lomítko je literál' => ['C:\\', 'c:\\\\'];
        yield 'podtržítko je literál' => ['a_b', 'a_b'];
        yield 'kombinace' => ['%a*b%', '*a\\*b*'];
        yield 'závorky projdou' => ['%(x)%', '*(x)*'];
    }


    /**
     * Hodnoty se od té doby, co se do textu podmínky nevkládají, neořezávají. Kdyby se
     * neescapovaly, `*` zadaná uživatelem by procházela celý index.
     */
    #[DataProvider('valueProvider')]
    public function testWildcardCharactersInValueAreEscaped(string $value, string $expected): void
    {
        self::assertSame(['wildcard' => ['name' => $expected]], $this->client->buildQuery('name LIKE ?', [$value]));
    }


    /**
     * S klauzulí ESCAPE i bez ní platí na wildcard znaky totéž.
     */
    public function testEscapingIsTheSameWithAndWithoutEscapeClause(): void
    {
        self::assertSame(
            $this->client->buildQuery("name LIKE ? ESCAPE '~'", ['a*b']),
            $this->client->buildQuery('name LIKE ?', ['a*b']),
        );
    }


    public function testNotLikeEscapesTheSameWay(): void
    {
        self::assertSame(
            ['bool' => ['must_not' => ['wildcard' => ['name' => 'a\\*b']]]],
            $this->client->buildQuery('name NOT LIKE ?', ['a*b']),
        );
    }
}
