<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Psr\Log\AbstractLogger;
use Stringable;

/**
 * PSR-3 logger, který si zapamatuje záznamy, aby se na ně dalo v testech ptát.
 */
final class RecordingLogger extends AbstractLogger
{
    /** @var list<array{level: string, message: string, context: mixed[]}> */
    public array $records = [];


    /**
     * @param mixed $level
     * @param mixed[] $context
     */
    public function log($level, string|Stringable $message, array $context = []): void
    {
        $this->records[] = [
            'level' => (string) $level,
            'message' => (string) $message,
            'context' => $context,
        ];
    }
}
