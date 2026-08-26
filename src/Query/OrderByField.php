<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use Hovjacky\NoSQL\DB;

/**
 * Jeden sloupec v orderBy, tj. název sloupce a směr řazení.
 * Vstupem je zápis používaný v parametrech findBy(), např. `id` nebo `id desc`.
 */
final class OrderByField
{
    private function __construct(
        public readonly string $column,
        public readonly bool $descending,
    )
    {
    }


    public static function fromString(string $definition): self
    {
        $desc = strpos($definition, DB::ORDER_BY_DESC_POSTFIX);

        return $desc !== false
            ? new self(substr($definition, 0, $desc), true)
            : new self($definition, false);
    }


    /**
     * Směr řazení tak, jak ho očekává Elasticsearch.
     */
    public function direction(): string
    {
        return $this->descending ? 'desc' : 'asc';
    }
}
