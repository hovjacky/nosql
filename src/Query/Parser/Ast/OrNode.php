<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser\Ast;

final class OrNode implements Node
{
    /**
     * @param list<Node> $operands vždy alespoň dva
     * @param bool $wrapOperands Mají se jednotlivé výrazy obalit do AND klauzule?
     *      Zachovává tvar dotazu, který knihovna generovala dřív: v podmínce bez závorek
     *      se každý operand OR obaluje do `bool.filter`, v podmínce se závorkami ne.
     *      Obojí vrací stejné dokumenty, liší se ale skóre, proto se rozdíl drží.
     */
    public function __construct(
        public readonly array $operands,
        public readonly bool $wrapOperands,
    )
    {
    }
}
