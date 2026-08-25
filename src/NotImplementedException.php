<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use Exception;
use Throwable;

class NotImplementedException extends Exception implements NoSQLException
{
    private const DEFAULT_MESSAGE = 'This feature has not yet been implemented.';


    public function __construct(string $message = '', int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message !== '' ? $message : self::DEFAULT_MESSAGE, $code, $previous);
    }
}
