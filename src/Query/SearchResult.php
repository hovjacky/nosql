<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query;

use ArrayIterator;
use IteratorAggregate;
use Traversable;

/**
 * Výsledek vyhledávání: nalezené záznamy spolu se surovou odpovědí z databáze.
 *
 * Nahrazuje předávání surové odpovědi referenčním parametrem findBy(), díky čemuž
 * se s výsledkem dá pracovat i tam, kde se reference špatně předávají (mocky, dekorátory).
 *
 * @implements IteratorAggregate<int, array<string, mixed>>
 */
final class SearchResult implements IteratorAggregate
{
    /**
     * @param array<int, array<string, mixed>> $rows nalezené záznamy
     * @param int|null $total celkový počet odpovídajících záznamů, pokud ho databáze uvedla
     * @param mixed[] $response surová odpověď z databáze
     */
    public function __construct(
        public readonly array $rows,
        public readonly ?int $total,
        public readonly array $response,
    )
    {
    }


    /**
     * Počet vrácených záznamů. Kvůli limitu může být menší než $total.
     */
    public function countRows(): int
    {
        return count($this->rows);
    }


    public function isEmpty(): bool
    {
        return $this->rows === [];
    }


    /**
     * První nalezený záznam, nebo null.
     * @return array<string, mixed>|null
     */
    public function first(): ?array
    {
        return $this->rows[0] ?? null;
    }


    /**
     * @return Traversable<int, array<string, mixed>>
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->rows);
    }
}
