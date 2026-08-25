<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DB;
use PHPUnit\Framework\TestCase;

/**
 * DB::count() je výchozí implementace pro databáze, které počítání neumí optimalizovat.
 */
final class GenericCountTest extends TestCase
{
    public function testCountAddsCountParamAndReturnsIntFromFindBy(): void
    {
        $db = new class([]) extends FakeDB {
            /**
             * @return array<int, array<string, mixed>>|int
             * @phpstan-ignore return.unusedType (signatura musí odpovídat rozhraní)
             */
            public function findBy(string $tableName, array $params): array|int
            {
                $this->lastParams = $params;

                return 42;
            }
        };

        self::assertSame(42, $db->count('t', ['where' => ['a = ?' => [1]]]));
        self::assertTrue($db->lastParams[DB::PARAM_COUNT]);
        self::assertArrayHasKey('where', $db->lastParams);
    }


    /**
     * Když databáze i s parametrem count vrátí záznamy, spočítáme je.
     */
    public function testCountFallsBackToCountingReturnedRows(): void
    {
        $db = new class([]) extends FakeDB {
            /**
             * @return array<int, array<string, mixed>>|int
             * @phpstan-ignore return.unusedType (signatura musí odpovídat rozhraní)
             */
            public function findBy(string $tableName, array $params): array|int
            {
                $this->lastParams = $params;

                return [['id' => 1], ['id' => 2], ['id' => 3]];
            }
        };

        self::assertSame(3, $db->count('t'));
    }
}
