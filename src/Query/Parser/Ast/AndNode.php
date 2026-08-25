<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser\Ast;

final class AndNode implements Node
{
    /**
     * @param list<Node> $operands vždy alespoň dva
     */
    public function __construct(public readonly array $operands)
    {
    }
}
