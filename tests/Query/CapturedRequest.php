<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Query;

use Exception;

/**
 * Vyhozena z modifyParamsCallback, aby se hotový dotaz dal odchytit ještě před odesláním do Elasticsearch.
 */
final class CapturedRequest extends Exception
{
    /**
     * @param array<string, mixed> $request
     */
    public function __construct(public readonly array $request)
    {
        parent::__construct('captured');
    }
}
