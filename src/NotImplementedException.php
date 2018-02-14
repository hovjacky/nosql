<?php

namespace Hovjacky\NoSQL;


use Throwable;

class NotImplementedException extends \Exception
{
    public function __construct(string $message = "", int $code = 0, Throwable $previous = null)
    {
        if (empty($message))
        {
            $message = "This feature has not yet been implemented.";
        }
        parent::__construct($message, $code, $previous);
    }

}