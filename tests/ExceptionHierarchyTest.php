<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use Hovjacky\NoSQL\NoSQLException;
use Hovjacky\NoSQL\NotImplementedException;
use PHPUnit\Framework\TestCase;

/**
 * Testy hierarchie výjimek a pojmenovaných konstruktorů DBException.
 */
final class ExceptionHierarchyTest extends TestCase
{
    public function testAllLibraryExceptionsShareMarkerInterface(): void
    {
        self::assertInstanceOf(NoSQLException::class, new DBException('chyba'));
        self::assertInstanceOf(NoSQLException::class, new NotImplementedException());
    }


    public function testRecordNotUpdatedFillsIdIntoMessage(): void
    {
        self::assertSame(
            'Záznam 42 nebyl nenalezen a tudíž ani upraven.',
            DBException::recordNotUpdated(42)->getMessage(),
        );
    }


    public function testRecordNotDeletedFillsIdIntoMessage(): void
    {
        self::assertSame(
            'Záznam abc-1 nebyl nenalezen a tudíž ani smazán.',
            DBException::recordNotDeleted('abc-1')->getMessage(),
        );
    }


    public function testRecordAlreadyExistsFillsIdIntoMessage(): void
    {
        self::assertSame('Záznam 7 již existuje.', DBException::recordAlreadyExists(7)->getMessage());
    }


    public function testFactoriesKeepUsingPublicMessageTemplates(): void
    {
        // Šablony zůstávají veřejným API na DB, továrny je jen plní.
        self::assertStringContainsString('{$id}', DB::ERROR_UPDATE);
        self::assertStringContainsString('{$id}', DB::ERROR_DELETE);
        self::assertStringContainsString('{$id}', DB::ERROR_BULK_INSERT_EXISTS);
    }


    public function testNotImplementedExceptionHasDefaultMessage(): void
    {
        self::assertSame('This feature has not yet been implemented.', (new NotImplementedException())->getMessage());
        self::assertSame('vlastní', (new NotImplementedException('vlastní'))->getMessage());
    }
}
