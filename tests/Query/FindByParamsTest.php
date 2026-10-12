<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use Hovjacky\NoSQL\Query\FindByParams;
use PHPUnit\Framework\TestCase;

/**
 * Testy kontroly a typování parametrů findBy().
 */
final class FindByParamsTest extends TestCase
{
    public function testEmptyParamsHaveNothingSet(): void
    {
        $params = FindByParams::fromArray([]);

        self::assertNull($params->fields);
        self::assertNull($params->limit);
        self::assertNull($params->offset);
        self::assertFalse($params->count);
        self::assertSame([], $params->orderBy);
        self::assertNull($params->groupBy);
        self::assertNull($params->groupByScript);
        self::assertSame([], $params->groupInternalOrderBy);
        self::assertSame([], $params->aggregation);
        self::assertSame([], $params->where);
    }


    public function testScalarOrderByBecomesList(): void
    {
        $params = FindByParams::fromArray(['orderBy' => 'id desc']);

        self::assertCount(1, $params->orderBy);
        self::assertSame('id', $params->orderBy[0]->column);
        self::assertTrue($params->orderBy[0]->descending);
    }


    public function testOrderByIsDroppedWhenCountingRecords(): void
    {
        $params = FindByParams::fromArray(['orderBy' => ['id'], 'groupInternalOrderBy' => ['x'], 'count' => true]);

        self::assertTrue($params->count);
        self::assertSame([], $params->orderBy);
        self::assertSame([], $params->groupInternalOrderBy);
    }


    public function testScalarAggregationColumnBecomesList(): void
    {
        self::assertSame(['max' => ['age']], FindByParams::fromArray(['aggregation' => ['max' => 'age']])->aggregation);
    }


    public function testScalarWhereValueIsWrapped(): void
    {
        self::assertSame(['id = ?' => [5]], FindByParams::fromArray(['where' => ['id = ?' => 5]])->where);
    }


    public function testNullWhereValueStaysNull(): void
    {
        self::assertSame(['x IS NULL' => null], FindByParams::fromArray(['where' => ['x IS NULL' => null]])->where);
    }


    public function testZeroLimitCountsAsNotSet(): void
    {
        self::assertNull(FindByParams::fromArray(['limit' => 0])->limit);
    }


    public function testNumericStringLimitIsCastToInt(): void
    {
        $params = FindByParams::fromArray(['limit' => '10', 'offset' => '5']);

        self::assertSame(10, $params->limit);
        self::assertSame(5, $params->offset);
    }


    public function testNonNumericOffsetCountsAsNotSet(): void
    {
        self::assertNull(FindByParams::fromArray(['offset' => 'abc'])->offset);
    }


    public function testEmptyFieldsCountAsNotSet(): void
    {
        self::assertNull(FindByParams::fromArray(['fields' => []])->fields);
        self::assertNull(FindByParams::fromArray(['fields' => 0])->fields);
    }


    public function testEmptyGroupByCountsAsNotSet(): void
    {
        self::assertNull(FindByParams::fromArray(['groupBy' => ''])->groupBy);
        self::assertNull(FindByParams::fromArray(['groupBy' => 0])->groupBy);
    }


    public function testEmptyGroupByScriptCountsAsNotSet(): void
    {
        self::assertNull(FindByParams::fromArray(['groupBy' => 'script', 'groupByScript' => ''])->groupByScript);
    }


    public function testNonArrayFieldsAreRejected(): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage(DB::ERROR_FIELDS);

        FindByParams::fromArray(['fields' => 'id']);
    }


    public function testNonArrayWhereIsRejected(): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage(DB::ERROR_WHERE);

        FindByParams::fromArray(['where' => 'x']);
    }


    public function testNumericWhereKeyIsRejected(): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage(DB::ERROR_WHERE);

        FindByParams::fromArray(['where' => [0 => 'x']]);
    }


    public function testNonArrayAggregationIsRejected(): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage(DB::ERROR_AGGREGATION);

        FindByParams::fromArray(['aggregation' => 'x']);
    }


    public function testToArrayKeepsUnknownKeysAndOrder(): void
    {
        $input = ['where' => ['a = ?' => 1], 'limit' => 5, 'somethingCustom' => 'x', 'orderBy' => 'y desc'];

        $result = FindByParams::fromArray($input)->toArray();

        self::assertSame(['where', 'limit', 'somethingCustom', 'orderBy'], array_keys($result));
        self::assertSame('x', $result['somethingCustom']);
        // toArray() vrací původní (znormalizovaný) tvar, ne typovaný.
        self::assertSame(['a = ?' => [1]], $result['where']);
        self::assertSame(['y desc'], $result['orderBy']);
        self::assertSame(5, $result['limit']);
    }


    /**
     * Záporný limit ani offset nedávají smysl a Elasticsearch by je odmítl chybou 400,
     * takže se berou jako neuvedené.
     */
    public function testNegativeLimitAndOffsetAreIgnored(): void
    {
        $params = FindByParams::fromArray(['limit' => -5, 'offset' => -1]);

        self::assertNull($params->limit);
        self::assertNull($params->offset);

        self::assertNull(FindByParams::fromArray(['limit' => '-5'])->limit);
    }


    public function testToArrayLeavesLimitTypeUntouched(): void
    {
        // Typovaný přístup limit přetypuje, původní tvar pro checkAndRepairParams zůstává beze změny.
        $params = FindByParams::fromArray(['limit' => '10']);

        self::assertSame(10, $params->limit);
        self::assertSame('10', $params->toArray()['limit']);
    }
}
