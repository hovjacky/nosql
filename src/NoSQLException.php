<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use Throwable;

/**
 * Společný marker všech výjimek této knihovny.
 * Umožňuje odchytit jakoukoliv chybu knihovny jediným `catch (NoSQLException $e)`
 * bez ohledu na to, zda jde o DBException nebo NotImplementedException.
 */
interface NoSQLException extends Throwable
{
}
