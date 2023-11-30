<?php

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
	public function getClient();


	/**
	 * Vloží záznam do tabulky.
	 * Pokud již existuje, tak ho upraví.
     * @param string $tableName
	 * @param array $data
	 * @return mixed true nebo false podle úspěchu
	 * @throws Throwable
	 */
	public function insert($tableName, $data);


	/**
	 * Hromadně vloží data do tabulky.
	 * Pokud již záznam existuje, tak ho upraví.
     * @param string $tableName
	 * @param array $data
	 * @return mixed true nebo false podle úspěchu
	 * @throws Throwable
	 */
	public function bulkInsert($tableName, $data);


	/**
	 * Přečte záznam z tabulky.
     * @param string $tableName
	 * @param int $id
	 * @return array|false Nalezený záznam nebo false
	 * @throws Throwable
	 */
	public function get($tableName, $id);


	/**
	 * Upraví záznam v tabulce.
     * @param string $tableName
	 * @param int $id
	 * @param array $data
	 * @return mixed true pokud byl záznam upraven, false pokud nebyl upraven
	 * @throws Throwable
	 */
	public function update($tableName, $id, $data);


	/**
	 * Smaže záznam z tabulky.
     * @param string $tableName
	 * @param int $id
	 * @return mixed true pokud byl záznam smazán
	 * @throws Throwable
	 */
	public function delete($tableName, $id);


	/**
	 * Smaže všechny záznamy z tabulky.
     * @param string $tableName
	 * @return bool
	 * @throws Throwable
	 */
	public function deleteAll($tableName);


	/**
	 * Vrátí záznamy odpovídající daným kritériím
     * @param string $tableName
	 * @param array $params
	 * @return int|array
	 * @throws Throwable
	 */
	public function findBy($tableName, $params);


	/**
	 * Překonvertuje některé datové typy pro databázi
	 * @param array $data
	 * @return array
	 */
	public function convertToDBDataTypes($data);


	/**
	 * Překonvertuje některé datové typy pro PHP
	 * @param array $data
	 * @return array
	 */
	public function convertFromDBDataTypes($data);
}
