<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query\Parser;

use Hovjacky\NoSQL\DBException;
use Hovjacky\NoSQL\Query\Parser\Ast\AndNode;
use Hovjacky\NoSQL\Query\Parser\Ast\ComparisonNode;
use Hovjacky\NoSQL\Query\Parser\Ast\Node;
use Hovjacky\NoSQL\Query\Parser\Ast\OrNode;
use Hovjacky\NoSQL\Query\Parser\ConditionParser;
use Hovjacky\NoSQL\Query\Parser\Tokenizer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Testy sestavení stromu where podmínky.
 */
final class ConditionParserTest extends TestCase
{
    private static function parse(string $query): Node
    {
        return ConditionParser::parse(Tokenizer::tokenize($query));
    }


    /**
     * Vypíše strom v závorkovém zápisu, ať se dá jednoduše porovnat.
     */
    private static function describe(Node $node): string
    {
        if ($node instanceof ComparisonNode)
        {
            return $node->expression;
        }

        if ($node instanceof AndNode)
        {
            return 'AND(' . implode(', ', array_map(self::describe(...), $node->operands)) . ')';
        }

        self::assertInstanceOf(OrNode::class, $node);

        return 'OR(' . implode(', ', array_map(self::describe(...), $node->operands)) . ')';
    }


    /**
     * @return iterable<string, array{string, string}>
     */
    public static function treeProvider(): iterable
    {
        yield 'jediný výraz' => ['a', 'a'];
        yield 'AND' => ['a AND b', 'AND(a, b)'];
        yield 'OR' => ['a OR b', 'OR(a, b)'];
        yield 'zřetězené AND' => ['a AND b AND c', 'AND(a, b, c)'];
        yield 'zřetězené OR' => ['a OR b OR c', 'OR(a, b, c)'];

        // AND váže silněji než OR, stejně jako v SQL.
        yield 'AND má přednost' => ['a AND b OR c', 'OR(AND(a, b), c)'];
        yield 'AND má přednost i vpravo' => ['a OR b AND c', 'OR(a, AND(b, c))'];
        yield 'obojí' => ['a AND b OR c AND d', 'OR(AND(a, b), AND(c, d))'];

        // Závorky přednost přebijí.
        yield 'závorky mění pořadí' => ['(a OR b) AND c', 'AND(OR(a, b), c)'];
        yield 'závorky vpravo' => ['a AND (b OR c)', 'AND(a, OR(b, c))'];
        yield 'zbytečné závorky' => ['(a)', 'a'];
        yield 'dvojité závorky' => ['((a))', 'a'];
        yield 'hluboké zanoření' => ['(((((a)))))', 'a'];
        yield 'zanořené skupiny' => ['a AND (b OR (c AND d))', 'AND(a, OR(b, AND(c, d)))'];
        yield 'dvě skupiny' => ['(a OR b) AND (c OR d)', 'AND(OR(a, b), OR(c, d))'];
        yield 'skupina a dva výrazy' => ['(a OR b) AND c AND d', 'AND(OR(a, b), c, d)'];
        yield 'dva výrazy a skupina' => ['a AND b AND (c OR d)', 'AND(a, b, OR(c, d))'];

        // Přednost platí i tam, kde se závorky mísí s nezávorkovanými spojkami.
        // Původní parser tuhle podmínku počítal jako `a AND (b OR c)`, viz README.
        yield 'skupina, AND a OR' => ['(a) AND b OR c', 'OR(AND(a, b), c)'];
        yield 'OR a skupina v AND' => ['a OR b AND (c)', 'OR(a, AND(b, c))'];
    }


    #[DataProvider('treeProvider')]
    public function testTree(string $query, string $expected): void
    {
        self::assertSame($expected, self::describe(self::parse($query)));
    }


    /**
     * @return iterable<string, array{string}>
     */
    public static function incompleteProvider(): iterable
    {
        yield 'prázdná podmínka' => [''];
        yield 'AND na konci' => ['a AND'];
        yield 'OR na konci' => ['a OR'];
        yield 'AND na začátku' => ['AND a'];
        yield 'OR na začátku' => ['OR a'];
        yield 'dvě spojky za sebou' => ['a AND OR b'];
        yield 'prázdné závorky' => ['()'];
        yield 'jen prázdné závorky vedle sebe' => ['() ()'];
    }


    #[DataProvider('incompleteProvider')]
    public function testIncompleteConditionIsRejected(string $query): void
    {
        $this->expectException(DBException::class);

        self::parse($query);
    }


    /**
     * Prázdnou skupinu generují stavitelé podmínek, když jim skupina filtrů vyjde prázdná.
     * Ať je kdekoliv, chová se, jako by tam nebyla.
     * @return iterable<string, array{string, string}>
     */
    public static function emptyGroupProvider(): iterable
    {
        yield 'na začátku' => ['() a', 'a'];
        yield 'na konci' => ['a AND ()', 'a'];
        yield 'na konci za skupinou' => ['(a) AND ()', 'a'];
        yield 'na konci u OR' => ['a OR ()', 'a'];
        yield 'na začátku před spojkou' => ['() AND a', 'a'];
        yield 'uprostřed' => ['a AND () AND b', 'AND(a, b)'];
        yield 'obě strany' => ['() a AND ()', 'a'];
    }


    #[DataProvider('emptyGroupProvider')]
    public function testEmptyGroupIsSkipped(string $query, string $expected): void
    {
        self::assertSame($expected, self::describe(self::parse($query)));
    }


    /**
     * Bez závorek se operandy OR balí do AND klauzule, se závorkami ne.
     * Rozdíl je historický, ale mění skóre, proto ho parser drží.
     */
    public function testWrapOperandsFlag(): void
    {
        $withoutParentheses = self::parse('a OR b');
        self::assertInstanceOf(OrNode::class, $withoutParentheses);
        self::assertTrue($withoutParentheses->wrapOperands);

        $withParentheses = self::parse('(a) OR b');
        self::assertInstanceOf(OrNode::class, $withParentheses);
        self::assertFalse($withParentheses->wrapOperands);

        // Vnitřní OR má vlastní úsek bez závorek, takže se balí.
        $nested = self::parse('(a OR b) AND c');
        self::assertInstanceOf(AndNode::class, $nested);
        $inner = $nested->operands[0];
        self::assertInstanceOf(OrNode::class, $inner);
        self::assertTrue($inner->wrapOperands);
    }


    /**
     * Rozhoduje se pro celý úsek OR najednou, ne pro každý operand zvlášť - jinak by
     * se v jednom OR míchaly skórované a neskórované větve.
     */
    public function testWrapOperandsIsDecidedForTheWholeOrSpan(): void
    {
        $mixed = self::parse('(a) OR b OR c');

        self::assertInstanceOf(OrNode::class, $mixed);
        self::assertCount(3, $mixed->operands);
        self::assertFalse($mixed->wrapOperands);
    }
}
