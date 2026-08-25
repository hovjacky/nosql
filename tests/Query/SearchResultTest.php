<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query;

use Hovjacky\NoSQL\Query\SearchResult;
use PHPUnit\Framework\TestCase;

/**
 * Testy objektu s výsledkem vyhledávání.
 */
final class SearchResultTest extends TestCase
{
    public function testExposesRowsTotalAndRawResponse(): void
    {
        $rows = [['id' => 1], ['id' => 2]];
        $response = ['hits' => ['total' => ['value' => 17]]];

        $result = new SearchResult($rows, 17, $response);

        self::assertSame($rows, $result->rows);
        self::assertSame(17, $result->total);
        self::assertSame($response, $result->response);
    }


    public function testCountRowsCountsReturnedRowsNotTotal(): void
    {
        $result = new SearchResult([['id' => 1], ['id' => 2]], 500, []);

        self::assertSame(2, $result->countRows());
        self::assertSame(500, $result->total);
    }


    public function testIsEmptyAndFirst(): void
    {
        $empty = new SearchResult([], 0, []);

        self::assertTrue($empty->isEmpty());
        self::assertNull($empty->first());

        $filled = new SearchResult([['id' => 1], ['id' => 2]], 2, []);

        self::assertFalse($filled->isEmpty());
        self::assertSame(['id' => 1], $filled->first());
    }


    public function testIsIterable(): void
    {
        $result = new SearchResult([['id' => 1], ['id' => 2]], 2, []);

        self::assertSame([['id' => 1], ['id' => 2]], iterator_to_array($result));
    }


    public function testTotalCanBeUnknown(): void
    {
        self::assertNull((new SearchResult([], null, []))->total);
    }
}
