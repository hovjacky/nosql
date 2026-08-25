<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

/**
 * Testy předávání hodnot do where podmínky.
 *
 * Hodnoty se do textu podmínky nevkládají, jen se na ně odkazuje značkou. Nemusí se proto
 * ořezávat (dřív se z nich mazaly „nebezpečné“ znaky) a zachovávají si svůj typ.
 */
final class BoundValuesTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    /**
     * @return iterable<string, array{string}>
     */
    public static function specialCharacterProvider(): iterable
    {
        yield 'závorky' => ['x(y)z'];
        yield 'apostrof' => ["O'Brien"];
        yield 'uvozovky' => ['say "hi"'];
        yield 'operátory' => ['a<b>c=d!=e'];
        yield 'lomítka' => ['a/b\\c'];
        yield 'dolar a zpětné lomítko' => ['$1 \\2'];
        yield 'otazník' => ['what?'];
        yield 'nový řádek' => ["line1\nline2"];
        yield 'diakritika' => ['Žluťoučký kůň'];
        yield 'emoji' => ['café ☕'];
        yield 'hranaté závorky' => ['[not a list]'];
        yield 'text se spojkou AND' => ['x AND y'];
        yield 'text se spojkou OR' => ['x OR y'];
        yield 'text připomínající značku' => ['#0#'];
        yield 'středník a komentář' => ['a; -- b'];
    }


    /**
     * Hodnota se do dotazu dostane celá a beze změny.
     */
    #[DataProvider('specialCharacterProvider')]
    public function testValueIsPassedThroughUntouched(string $value): void
    {
        self::assertSame(['match' => ['name' => $value]], $this->client->buildQuery('name = ?', [$value]));
    }


    #[DataProvider('specialCharacterProvider')]
    public function testValueWithSpecialCharactersCombinesWithOtherConditions(string $value): void
    {
        self::assertSame(
            ['bool' => ['filter' => [['match' => ['name' => $value]], ['match' => ['age' => 30]]]]],
            $this->client->buildQuery('name = ? AND age = ?', [$value, 30]),
        );
    }


    /**
     * @return iterable<string, array{mixed}>
     */
    public static function typeProvider(): iterable
    {
        yield 'int' => [30];
        yield 'číselný řetězec' => ['30'];
        yield 'float' => [1.5];
        yield 'desetinný řetězec' => ['1.5'];
        yield 'řetězec s nulou na začátku' => ['007'];
        yield 'true' => [true];
        yield 'false' => [false];
        yield 'prázdný řetězec' => [''];
    }


    /**
     * Hodnota si drží typ, se kterým ji volající předal.
     */
    #[DataProvider('typeProvider')]
    public function testValueKeepsItsType(mixed $value): void
    {
        self::assertSame(['match' => ['a' => $value]], $this->client->buildQuery('a = ?', [$value]));
    }


    /**
     * Text zapsaný přímo v podmínce žádný typ nemá, ten se odhaduje ze zápisu.
     */
    public function testInlineTextIsStillNormalized(): void
    {
        self::assertSame(['match' => ['a' => 18]], $this->client->buildQuery('a = 18'));
        self::assertSame(['match' => ['a' => 1.5]], $this->client->buildQuery('a = 1.5'));
        self::assertSame(['match' => ['a' => '007']], $this->client->buildQuery('a = 007'));
        self::assertSame(['terms' => ['a' => [1, 2, 3]]], $this->client->buildQuery('a = [1,2,3]'));
    }


    public function testListValuesKeepTheirTypes(): void
    {
        self::assertSame(
            ['terms' => ['a' => [1, 'x,y', 2.5]]],
            $this->client->buildQuery('a IN ?', [[1, 'x,y', 2.5]]),
        );
    }


    public function testLikeValueKeepsSpecialCharacters(): void
    {
        self::assertSame(['wildcard' => ['a' => '*(x)*']], $this->client->buildQuery('a LIKE ?', ['%(x)%']));
        self::assertSame(['wildcard' => ['a' => '*a and b*']], $this->client->buildQuery('a LIKE ?', ['%a AND b%']));
    }


    public function testCrossFieldsValueKeepsSpecialCharacters(): void
    {
        self::assertSame(
            [
                'multi_match' => [
                    'query' => 'Smith AND Jones (jr)',
                    'type' => 'cross_fields',
                    'operator' => 'and',
                    'fields' => ['f', 'g'],
                ],
            ],
            $this->client->buildQuery('f,g CROSS FIELDS ?', ['Smith AND Jones (jr)']),
        );
    }


    /**
     * Řetězec s hranatými závorkami je pořád jen řetězec, ne seznam.
     */
    public function testStringWithBracketsIsNotTreatedAsList(): void
    {
        self::assertSame(
            ['match' => ['a' => '[not a list]']],
            $this->client->buildQuery('a = ?', ['[not a list]']),
        );
    }


    /**
     * @return iterable<string, array{mixed, string}>
     */
    public static function unsupportedValueProvider(): iterable
    {
        yield 'null' => [null, 'null'];
        yield 'objekt' => [new stdClass(), 'stdClass'];
    }


    #[DataProvider('unsupportedValueProvider')]
    public function testUnsupportedValueTypeIsRejected(mixed $value, string $type): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage('Hodnotou filtru nemůže být ' . $type . '.');

        $this->client->buildQuery('a = ?', [$value]);
    }


    public function testInWithScalarValueReportsTheActualValue(): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage("zadáno: `int 5`");

        $this->client->buildQuery('a IN ?', [5]);
    }


    /**
     * Značky si generuje knihovna, v zadané podmínce nesmí být.
     */
    public function testReservedSequenceInConditionIsRejected(): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage(DB::ERROR_BOOLEAN_RESERVED_SEQUENCE);

        $this->client->buildQuery('a = #0# AND b = ?', ['x']);
    }


    public function testConditionWithoutValuesMayContainAnything(): void
    {
        // Bez hodnot se nic nenahrazuje, takže není co zaměnit.
        self::assertSame(['match' => ['a' => '#0#']], $this->client->buildQuery('a = #0#'));
    }
}
