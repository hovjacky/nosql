<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query\Parser;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use Hovjacky\NoSQL\Query\Parser\Token;
use Hovjacky\NoSQL\Query\Parser\Tokenizer;
use Hovjacky\NoSQL\Query\Parser\TokenType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Testy rozdělení where podmínky na tokeny.
 */
final class TokenizerTest extends TestCase
{
    /**
     * @param list<Token> $tokens
     * @return list<string>
     */
    private static function describe(array $tokens): array
    {
        return array_map(
            static fn (Token $token): string => $token->type === TokenType::Expression
                ? 'expr(' . $token->value . ')'
                : $token->type->name,
            $tokens,
        );
    }


    /**
     * @return iterable<string, array{string, list<string>}>
     */
    public static function tokenProvider(): iterable
    {
        yield 'jediný výraz' => ['a = 1', ['expr(a = 1)']];
        yield 'AND' => ['a = 1 AND b = 2', ['expr(a = 1)', 'And_', 'expr(b = 2)']];
        yield 'OR' => ['a = 1 OR b = 2', ['expr(a = 1)', 'Or_', 'expr(b = 2)']];
        yield 'závorky' => [
            '(a = 1)',
            ['OpeningParenthesis', 'expr(a = 1)', 'ClosingParenthesis'],
        ];
        yield 'kombinace' => [
            'a = 1 AND (b = 2 OR c = 3)',
            ['expr(a = 1)', 'And_', 'OpeningParenthesis', 'expr(b = 2)', 'Or_', 'expr(c = 3)', 'ClosingParenthesis'],
        ];
        yield 'prázdný vstup' => ['', []];
        yield 'přebytečné mezery' => ['  a = 1   AND   b = 2  ', ['expr(a = 1)', 'And_', 'expr(b = 2)']];

        // Klíčové slovo musí stát samostatně, ne jako část jiného slova.
        yield 'ANDROID není AND' => ['os = ANDROID', ['expr(os = ANDROID)']];
        yield 'BRAND není AND' => ['BRAND = x', ['expr(BRAND = x)']];
        yield 'ORDER není OR' => ['ORDER = 1', ['expr(ORDER = 1)']];
        yield 'malé and se nebere' => ['a = 1 and b = 2', ['expr(a = 1 and b = 2)']];

        // Literál v apostrofech je nedělitelný.
        yield 'AND v literálu' => ["name LIKE '%a AND b%'", ["expr(name LIKE '%a AND b%')"]];
        yield 'závorka v literálu' => ["name LIKE '%(x)%'", ["expr(name LIKE '%(x)%')"]];
        yield 'literál a za ním AND' => [
            "name LIKE '%a AND b%' AND age = 1",
            ["expr(name LIKE '%a AND b%')", 'And_', 'expr(age = 1)'],
        ];
        yield 'klauzule ESCAPE' => [
            "name LIKE ? ESCAPE '~' OR name IS NULL",
            ["expr(name LIKE ? ESCAPE '~')", 'Or_', 'expr(name IS NULL)'],
        ];
        yield 'neuzavřený literál' => ["name LIKE 'abc", ["expr(name LIKE 'abc)"]];
    }


    /**
     * @param list<string> $expected
     */
    #[DataProvider('tokenProvider')]
    public function testTokenize(string $query, array $expected): void
    {
        self::assertSame($expected, self::describe(Tokenizer::tokenize($query)));
    }


    /**
     * @return iterable<string, array{string}>
     */
    public static function unbalancedProvider(): iterable
    {
        yield 'chybí zavírací' => ['(a = 1 AND b = 2'];
        yield 'chybí otevírací' => ['a = 1 AND b = 2)'];
        yield 'zavírací první' => [')a = 1('];
    }


    #[DataProvider('unbalancedProvider')]
    public function testUnbalancedParenthesesAreRejected(string $query): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage(DB::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);

        Tokenizer::tokenize($query);
    }


    /**
     * Závorka uvnitř literálu není strukturou dotazu, takže se do párování nepočítá.
     */
    public function testParenthesisInsideLiteralDoesNotCount(): void
    {
        self::assertSame(["expr(name LIKE '(')"], self::describe(Tokenizer::tokenize("name LIKE '('")));
    }
}
