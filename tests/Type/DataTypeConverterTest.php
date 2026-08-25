<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Type;

use DateTime;
use Hovjacky\NoSQL\Type\DataTypeConverter;
use PHPUnit\Framework\Attributes\DataProvider;
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


    /**
     * @return iterable<string, array{string}>
     */
    public static function nonDateProvider(): iterable
    {
        yield 'text obsahující datum' => ['schůzka 2024-01-31 v Brně'];
        yield 'text obsahující datum s časem' => ['sraz 2024-01-31T10:00 u fontány'];
        yield 'datum s textem za ním' => ['2024-01-31T10:00 potvrzeno'];
        yield 'datum s textem před ním' => ['do 2024-01-31'];
        yield 'jen podobný tvar' => ['1234-56-78'];
        yield 'verze' => ['v2024-01-31'];
    }


    /**
     * Vzor musí být ukotvený - text, který datum jen obsahuje, se nesmí převádět.
     */
    #[DataProvider('nonDateProvider')]
    public function testStringsMerelyContainingADateAreNotConverted(string $value): void
    {
        self::assertSame(['x' => $value], $this->converter->fromDatabase(['x' => $value]));
    }


    /**
     * @return iterable<string, array{string}>
     */
    public static function dateProvider(): iterable
    {
        yield 'datum' => ['2024-01-31'];
        yield 'datum a čas' => ['2024-01-31T12:30'];
        yield 'se sekundami' => ['2024-01-31T12:30:45'];
        yield 'se zlomkem sekundy' => ['2024-01-31T12:30:45.123'];
        yield 'se zónou Z' => ['2024-01-31T12:30:45Z'];
        yield 'se zónou s dvojtečkou' => ['2024-01-31T12:30:45+01:00'];
        yield 'se zónou bez dvojtečky' => ['2024-01-31T12:30:45+0100'];
    }


    #[DataProvider('dateProvider')]
    public function testRecognizedDateFormats(string $value): void
    {
        self::assertInstanceOf(DateTime::class, $this->converter->fromDatabase(['x' => $value])['x']);
    }


    public function testRoundTripKeepsDateValue(): void
    {
        $date = new DateTime('2024-01-31T12:30:00+00:00');

        $result = $this->converter->fromDatabase($this->converter->toDatabase(['d' => $date]));

        self::assertInstanceOf(DateTime::class, $result['d']);
        self::assertSame($date->getTimestamp(), $result['d']->getTimestamp());
    }
}
