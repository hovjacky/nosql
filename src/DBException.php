<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use Exception;

class DBException extends Exception implements NoSQLException
{
    /**
     * Záznam, který se měl upravit, v databázi neexistuje.
     */
    public static function recordNotUpdated(string|int $id): self
    {
        return new self(self::withId(DB::ERROR_UPDATE, $id));
    }


    /**
     * Záznam, který se měl smazat, v databázi neexistuje.
     */
    public static function recordNotDeleted(string|int $id): self
    {
        return new self(self::withId(DB::ERROR_DELETE, $id));
    }


    /**
     * Záznam, který se měl vložit, v databázi již existuje.
     */
    public static function recordAlreadyExists(string|int $id): self
    {
        return new self(self::withId(DB::ERROR_BULK_INSERT_EXISTS, $id));
    }


    /**
     * Doplní id do šablony chybové hlášky s placeholderem `{$id}`.
     */
    private static function withId(string $messageTemplate, string|int $id): string
    {
        return str_replace('{$id}', (string) $id, $messageTemplate);
    }
}
