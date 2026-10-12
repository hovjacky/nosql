<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser\Ast;

/**
 * List stromu - jeden výraz, např. `age >= 18`. Na operátory ho rozebírá až konkrétní databáze.
 */
final class ComparisonNode implements Node
{
    public function __construct(public readonly string $expression)
    {
    }
}
