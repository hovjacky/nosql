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
	 * @param array $data
	 * @return mixed true nebo false podle úspěchu
	 * @throws Throwable
	 */
	public function insert($data);


	/**
	 * Hromadně vloží data do tabulky.
	 * Pokud již záznam existuje, tak ho upraví.
	 * @param array $data
	 * @return mixed true nebo false podle úspěchu
	 * @throws Throwable
	 */
	public function bulkInsert($data);


	/**
	 * Přečte záznam z tabulky.
	 * @param int $id
	 * @return array|false Nalezený záznam nebo false
	 * @throws Throwable
	 */
	public function get($id);


	/**
	 * Upraví záznam v tabulce.
	 * @param int $id
	 * @param array $data
	 * @return mixed true pokud byl záznam upraven, false pokud nebyl upraven
	 * @throws Throwable
	 */
	public function update($id, $data);


	/**
	 * Smaže záznam z tabulky.
	 * @param int $id
	 * @return mixed true pokud byl záznam smazán
	 * @throws Throwable
	 */
	public function delete($id);


	/**
	 * Smaže všechny záznamy z tabulky.
	 * @return bool
	 * @throws Throwable
	 */
	public function deleteAll();


	/**
	 * Vrátí záznamy odpovídající daným kritériím
	 * @param array $params
	 * @return int|array
	 * @throws Throwable
	 */
	public function findBy($params);


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
