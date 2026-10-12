<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser;

final class Token
{
    public function __construct(
        public readonly TokenType $type,
        public readonly string $value = '',
    )
    {
    }
}
