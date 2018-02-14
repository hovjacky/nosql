<?php

namespace Hovjacky\NoSQL;

interface DBInterface {
	/**
	 * Vrátí klienta pro možnost vytvoření jakéhokoliv dotazu vázaného přímo na danou databázi.
	 *
	 * @return mixed
	 */
	public function getClient();

	/**
	 * Vloží záznam do tabulky.
	 * Pokud již existuje, tak ho upraví.
	 *
	 * @param string $table
	 * @param array $data
	 * @return mixed true nebo false podle úspěchu
	 * @throws \Exception
	 */
	public function insert($table, $data);

	/**
	 * Hromadně vloží data do tabulky.
	 * Pokud již záznam existuje, tak ho upraví.
	 *
	 * @param string $table
	 * @param array $data
	 * @return mixed true nebo false podle úspěchu
	 * @throws \Exception
	 */
	public function bulkInsert($table, $data);

	/**
	 * Přečte záznam z tabulky.
	 *
	 * @param string $table
	 * @param int $id
	 * @return array|bool Nalezený záznam nebo false
	 * @throws \Exception
	 */
	public function get($table, $id);

	/**
	 * Upraví záznam v tabulce.
	 *
	 * @param string $table
	 * @param int $id
	 * @param array $data
	 * @return mixed true pokud byl záznam upraven, false pokud nebyl upraven
	 * @throws \Exception
	 */
	public function update($table, $id, $data);

	/**
	 * Smaže záznam z tabulky.
	 *
	 * @param string $table
	 * @param int $id
	 * @return mixed true pokud byl záznam smazán
	 * @throws \Exception
	 */
	public function delete($table, $id);

	/**
	 * Smaže všechny záznamy z tabulky.
	 *
	 * @param string $table
	 * @return bool
	 * @throws \Exception
	 */
	public function deleteAll($table);

	/**
	 * Vrátí záznamy odpovídající daným kritériím
	 *
	 * @param string $table
	 * @param array $params
	 * @return int|array
	 * @throws \Exception
	 */
	public function findBy($table, $params);

	/**
	 * Překonvertuje některé datové typy pro databázi
	 *
	 * @param $data array
	 * @return array
	 */
	public function convertToDBDataTypes($data);

	/**
	 * Překonvertuje některé datové typy pro PHP
	 *
	 * @param $data array
	 * @return array
	 */
	public function convertFromDBDataTypes($data);
}