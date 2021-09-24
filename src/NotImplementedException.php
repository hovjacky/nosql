<?php

namespace Hovjacky\NoSQL;

use Exception;
use Throwable;

/**
 * Class NotImplementedException
 * @package Hovjacky\NoSQL
 */
class NotImplementedException extends Exception
{
    /**
     * NotImplementedException constructor.
     * @param string $message
     * @param int $code
     * @param Throwable|null $previous
     */
    public function __construct(string $message = "", int $code = 0, Throwable $previous = null)
    {
        if (empty($message))
        {
            $message = "This feature has not yet been implemented.";
        }

        parent::__construct($message, $code, $previous);
    }
}
