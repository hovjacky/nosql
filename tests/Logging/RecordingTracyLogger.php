<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Logging;

use Tracy\ILogger;

/**
 * Tracy logger, který si zapamatuje záznamy místo zápisu na disk.
 */
final class RecordingTracyLogger implements ILogger
{
    /** @var list<array{value: mixed, level: string}> */
    public array $records = [];


    public function log(mixed $value, string $level = self::INFO): void
    {
        $this->records[] = ['value' => $value, 'level' => $level];
    }
}
