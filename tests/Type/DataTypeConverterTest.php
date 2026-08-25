<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Type;

use DateTime;
use Hovjacky\NoSQL\Type\DataTypeConverter;
use PHPUnit\Framework\TestCase;

/**
 * Testy převodu hodnot mezi PHP a Elasticsearch.
 */
final class DataTypeConverterTest extends TestCase
{
    private DataTypeConverter $converter;


    protected function setUp(): void
    {
        $this->converter = new DataTypeConverter();
    }


    public function testDateTimeIsFormattedForDatabase(): void
    {
        $date = new DateTime('2024-01-31T12:00:00+00:00');

        self::assertSame(['created' => '2024-01-31T12:00:00+00:00'], $this->converter->toDatabase(['created' => $date]));
    }


    public function testNestedArraysAreConvertedForDatabase(): void
    {
        $date = new DateTime('2024-01-31T12:00:00+00:00');

        self::assertSame(
            ['sub' => ['created' => '2024-01-31T12:00:00+00:00', 'n' => 5]],
            $this->converter->toDatabase(['sub' => ['created' => $date, 'n' => 5]]),
        );
    }


    public function testNonDateValuesArePassedThroughUnchanged(): void
    {
        $data = ['n' => 5, 'f' => 1.5, 'b' => true, 's' => 'ahoj', 'null' => null];

        self::assertSame($data, $this->converter->toDatabase($data));
        self::assertSame($data, $this->converter->fromDatabase($data));
    }


    public function testDateStringBecomesDateTime(): void
    {
        $result = $this->converter->fromDatabase(['d' => '2024-01-31']);

        self::assertInstanceOf(DateTime::class, $result['d']);
        self::assertSame('2024-01-31', $result['d']->format('Y-m-d'));
    }


    public function testDateTimeStringBecomesDateTime(): void
    {
        $result = $this->converter->fromDatabase(['d' => '2024-01-31T12:30:00+00:00']);

        self::assertInstanceOf(DateTime::class, $result['d']);
        self::assertSame('2024-01-31 12:30', $result['d']->format('Y-m-d H:i'));
    }


    public function testTenCharacterNonDateIsNotConverted(): void
    {
        self::assertSame(['s' => 'abcdefghij'], $this->converter->fromDatabase(['s' => 'abcdefghij']));
    }


    public function testNestedArraysAreConvertedFromDatabase(): void
    {
        $result = $this->converter->fromDatabase(['sub' => ['d' => '2024-01-31', 'n' => 5]]);

        self::assertInstanceOf(DateTime::class, $result['sub']['d']);
        self::assertSame(5, $result['sub']['n']);
    }


    public function testRoundTripKeepsDateValue(): void
    {
        $date = new DateTime('2024-01-31T12:30:00+00:00');

        $result = $this->converter->fromDatabase($this->converter->toDatabase(['d' => $date]));

        self::assertInstanceOf(DateTime::class, $result['d']);
        self::assertSame($date->getTimestamp(), $result['d']->getTimestamp());
    }
}
