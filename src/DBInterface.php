<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use Throwable;

/**
 * Class DBInterface
 * @package Hovjacky\NoSQL
 */
interface DBInterface
{
    /**
     * Vrátí klienta pro možnost vytvoření jakéhokoliv dotazu vázaného přímo na danou databázi.
     * @return mixed
     */
    public function getClient(): mixed;


    /**
     * Vloží záznam do tabulky.
     * Pokud již existuje, tak ho upraví.
     * @param string $tableName
     * @param array<string, mixed> $data Sloupec -> hodnota
     * @return bool Vrací, zda byl záznam založen/upraven
     * @throws Throwable
     */
    public function insertOrUpdate(string $tableName, array $data): bool;


    /**
     * Hromadně vloží data do tabulky.
     * Pokud již záznam existuje, tak ho upraví.
     * @param string $tableName
     * @param array<int, array<string, mixed>> $data Řádky k uložení: Sloupec -> hodnota
     * @return bool Vrací, zda byly záznamy založeny/upraveny
     * @throws Throwable
     */
    public function bulkInsertOrUpdate(string $tableName, array $data): bool;


    /**
     * Přečte záznam z tabulky.
     * @param string $tableName
     * @param int $id
     * @return array<string, mixed>|null Vrací nalezený záznam nebo null
     * @throws Throwable
     */
    public function get(string $tableName, int $id): ?array;


    /**
     * Upraví záznam v tabulce.
     * @param string $tableName
     * @param int $id
     * @param array<string, mixed> $data Sloupec -> hodnota
     * @return bool Vrací, zda byl záznam upraven
     * @throws Throwable
     */
    public function update(string $tableName, int $id, array $data): bool;


    /**
     * Smaže záznam z tabulky.
     * @param string $tableName
     * @param int $id
     * @return bool true pokud byl záznam smazán, jinak vyhodí výjimku
     * @throws Throwable
     */
    public function delete(string $tableName, int $id): bool;


    /**
     * Smaže všechny záznamy z tabulky.
     * @param string $tableName
     * @return true true pokud byl záznam smazán, jinak vyhodí výjimku
     * @throws Throwable
     */
    public function deleteAll(string $tableName): true;


    /**
     * Vrátí záznamy odpovídající daným kritériím.
     * @param string $tableName
     * @param array<string, mixed> $params
     * @return array<int, array<string, mixed>>|int Nalezené záznamy nebo počet nalezených záznamů
     * @throws Throwable
     */
    public function findBy(string $tableName, array $params): array|int;


    /**
     * Překonvertuje některé datové typy pro databázi.
     * @param array<string, mixed> $data Sloupec -> hodnota
     * @return array<string, mixed> Sloupec -> upravená hodnota
     */
    public function convertToDBDataTypes(array $data): array;


    /**
     * Překonvertuje některé datové typy pro PHP.
     * @param array<string, mixed> $data Sloupec -> hodnota
     * @return array<string, mixed> Sloupec -> upravená hodnota
     */
    public function convertFromDBDataTypes(array $data): array;
}
