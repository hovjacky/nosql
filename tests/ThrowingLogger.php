<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use LogicException;
use Psr\Log\AbstractLogger;
use Stringable;

/**
 * Logger, kterému se zápis nepovede - napodobuje Tracy bez nastaveného adresáře pro logy.
 */
final class ThrowingLogger extends AbstractLogger
{
    /**
     * @param mixed $level
     * @param mixed[] $context
     */
    public function log($level, string|Stringable $message, array $context = []): void
    {
        throw new LogicException('Logging directory is not specified.');
    }
}
