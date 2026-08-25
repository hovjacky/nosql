<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DB;

/**
 * Minimální potomek DB pro testy společné logiky v základní třídě.
 */
abstract class FakeDB extends DB
{
    /** @var array<string, mixed> parametry posledního volání findBy() */
    public array $lastParams = [];


    /**
     * @param array<string, scalar> $params
     * @phpstan-ignore constructor.unusedParameter
     */
    public function __construct(array $params)
    {
    }


    public function getClient(): mixed
    {
        return null;
    }


    public function insertOrUpdate(string $tableName, array $data): bool
    {
        return true;
    }


    public function bulkInsertOrUpdate(string $tableName, array $data): bool
    {
        return true;
    }


    public function get(string $tableName, string|int $id): ?array
    {
        return null;
    }


    public function update(string $tableName, string|int $id, array $data): bool
    {
        return true;
    }


    public function delete(string $tableName, string|int $id): bool
    {
        return true;
    }


    public function deleteAll(string $tableName): void
    {
    }


    public function convertToDBDataTypes(array $data): array
    {
        return $data;
    }


    public function convertFromDBDataTypes(array $data): array
    {
        return $data;
    }
}
