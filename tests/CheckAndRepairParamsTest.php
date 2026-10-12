<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * DB::checkAndRepairParams() je chráněný přípojný bod, který si potomci mohou přepsat.
 * Přestože normalizaci dnes dělá FindByParams, musí zůstat tvar výstupu beze změny.
 */
final class CheckAndRepairParamsTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    /**
     * @return iterable<string, array{array<string, mixed>, array<string, mixed>}>
     */
    public static function normalizationProvider(): iterable
    {
        yield 'prázdné' => [[], []];
        yield 'skalární orderBy' => [['orderBy' => 'id desc'], ['orderBy' => ['id desc']]];
        yield 'orderBy se zahodí u count' => [['orderBy' => ['id'], 'count' => true], ['count' => true]];
        yield 'skalární groupInternalOrderBy' => [
            ['groupInternalOrderBy' => 'id'],
            ['groupInternalOrderBy' => ['id']],
        ];
        yield 'skalární agregace' => [['aggregation' => ['max' => 'age']], ['aggregation' => ['max' => ['age']]]];
        yield 'agregace se sloupcem int' => [['aggregation' => ['min' => [5]]], ['aggregation' => ['min' => [5]]]];
        yield 'skalární where hodnota' => [['where' => ['id = ?' => 5]], ['where' => ['id = ?' => [5]]]];
        yield 'null where hodnota' => [['where' => ['x IS NULL' => null]], ['where' => ['x IS NULL' => null]]];
        yield 'limit zůstává jak přišel' => [['limit' => '10'], ['limit' => '10']];
        yield 'neznámé klíče projdou' => [['zzz' => ['a' => 1]], ['zzz' => ['a' => 1]]];
        yield 'prázdné hodnoty se nemění' => [
            ['fields' => [], 'where' => [], 'orderBy' => [], 'aggregation' => []],
            ['fields' => [], 'where' => [], 'orderBy' => [], 'aggregation' => []],
        ];
    }


    /**
     * @param array<string, mixed> $input
     * @param array<string, mixed> $expected
     */
    #[DataProvider('normalizationProvider')]
    public function testNormalization(array $input, array $expected): void
    {
        self::assertSame($expected, $this->client->repairParams($input));
    }


    /**
     * @return iterable<string, array{array<string, mixed>, string}>
     */
    public static function rejectedProvider(): iterable
    {
        yield 'fields' => [['fields' => 'id'], DB::ERROR_FIELDS];
        yield 'where' => [['where' => 'x'], DB::ERROR_WHERE];
        yield 'where číselný klíč' => [['where' => [0 => 'x']], DB::ERROR_WHERE];
        yield 'aggregation' => [['aggregation' => 'x'], DB::ERROR_AGGREGATION];
    }


    /**
     * @param array<string, mixed> $params
     */
    #[DataProvider('rejectedProvider')]
    public function testRejectedParams(array $params, string $expectedMessage): void
    {
        $this->expectException(DBException::class);
        $this->expectExceptionMessage($expectedMessage);

        $this->client->repairParams($params);
    }


    public function testKeyOrderIsPreserved(): void
    {
        $result = $this->client->repairParams(['where' => ['a = ?' => 1], 'limit' => 5, 'fields' => ['x']]);

        self::assertSame(['where', 'limit', 'fields'], array_keys($result));
    }
}
