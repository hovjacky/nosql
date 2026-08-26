<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query;

use Hovjacky\NoSQL\Query\OrderByField;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Testy rozpadu zápisu `sloupec [desc]` na název sloupce a směr řazení.
 */
final class OrderByFieldTest extends TestCase
{
    /**
     * @return iterable<string, array{string, string, bool}>
     */
    public static function definitionProvider(): iterable
    {
        yield 'bez přípony' => ['id', 'id', false];
        yield 'sestupně' => ['id desc', 'id', true];
        yield 'podtržítko v názvu' => ['created_at desc', 'created_at', true];
        // Přípona se hledá kdekoliv v řetězci, ne jen na konci - shodné s původní implementací.
        yield 'uprostřed názvu' => ['my desc column', 'my', true];
        yield 'jen desc' => [' desc', '', true];
        yield 'velká písmena se neberou' => ['id DESC', 'id DESC', false];
    }


    #[DataProvider('definitionProvider')]
    public function testParsing(string $definition, string $column, bool $descending): void
    {
        $field = OrderByField::fromString($definition);

        self::assertSame($column, $field->column);
        self::assertSame($descending, $field->descending);
        self::assertSame($descending ? 'desc' : 'asc', $field->direction());
    }
}
