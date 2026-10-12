<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser\Ast;

final class OrNode implements Node
{
    /**
     * @param list<Node> $operands vždy alespoň dva
     * @param bool $wrapOperands Mají se jednotlivé výrazy obalit do AND klauzule?
     *      Zachovává tvar dotazu, který knihovna generovala pro podmínku bez závorek:
     *      každý operand OR se obaluje do `bool.filter`. Jakmile je v úseku OR závorka,
     *      neobaluje se žádný operand - rozhoduje se pro celý úsek najednou, aby se
     *      skórované a neskórované větve OR nemíchaly. Obojí vrací stejné dokumenty,
     *      liší se jen skóre (viz README).
     */
    public function __construct(
        public readonly array $operands,
        public readonly bool $wrapOperands,
    )
    {
    }
}
