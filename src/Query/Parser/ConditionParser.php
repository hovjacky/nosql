<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use Hovjacky\NoSQL\Query\Parser\Ast\AndNode;
use Hovjacky\NoSQL\Query\Parser\Ast\ComparisonNode;
use Hovjacky\NoSQL\Query\Parser\Ast\Node;
use Hovjacky\NoSQL\Query\Parser\Ast\OrNode;

/**
 * Rekurzivní sestupný parser where podmínky.
 *
 * Gramatika (AND váže silněji než OR, stejně jako v SQL):
 *     or      := and (OR and)*
 *     and     := primary (AND primary)*
 *     primary := '(' or ')' | výraz
 *
 * Původní parser skládal dotaz řezáním řetězce a v podmínkách, kde se závorky mísí
 * s nezávorkovanými spojkami, přednost nedodržel (`(a) AND b OR c` počítal jako
 * `a AND (b OR c)`). Tady platí přednost SQL vždy, viz README.
 */
final class ConditionParser
{
    private const ERROR_INCOMPLETE = 'Neúplná podmínka - očekáván výraz.';


    /** @var list<Token> */
    private array $tokens;

    private int $position = 0;


    /**
     * @param list<Token> $tokens
     */
    private function __construct(array $tokens)
    {
        $this->tokens = $tokens;
    }


    /**
     * @param list<Token> $tokens
     * @throws DBException
     */
    public static function parse(array $tokens): Node
    {
        $parser = new self($tokens);

        $node = $parser->parseOr();

        if ($parser->position !== count($tokens))
        {
            throw new DBException(DB::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);
        }

        // Podmínka složená jen z prázdných závorek nemá co filtrovat.
        if ($node === null)
        {
            throw new DBException(self::ERROR_INCOMPLETE);
        }

        return $node;
    }


    /**
     * Vrací null, pokud celý úsek tvořily jen prázdné závorky.
     * @throws DBException
     * @phpstan-impure posouvá $this->position
     */
    private function parseOr(): ?Node
    {
        $start = $this->position;
        $operands = [];

        $this->collect($operands, $this->parseAnd());

        while ($this->peek()?->type === TokenType::Or_)
        {
            $this->position++;

            $this->collect($operands, $this->parseAnd());
        }

        if (count($operands) < 2)
        {
            return $operands[0] ?? null;
        }

        return new OrNode($operands, !$this->spanHasParentheses($start, $this->position));
    }


    /**
     * Vrací null, pokud celý úsek tvořily jen prázdné závorky.
     * @throws DBException
     * @phpstan-impure posouvá $this->position
     */
    private function parseAnd(): ?Node
    {
        $operands = [];

        $this->collect($operands, $this->parsePrimary());

        while ($this->peek()?->type === TokenType::And_)
        {
            $this->position++;

            $this->collect($operands, $this->parsePrimary());
        }

        if (count($operands) < 2)
        {
            return $operands[0] ?? null;
        }

        return new AndNode($operands);
    }


    /**
     * Vrací null pro prázdné závorky, které se v podmínce chovají, jako by tam nebyly.
     * @throws DBException
     * @phpstan-impure posouvá $this->position
     */
    private function parsePrimary(): ?Node
    {
        $token = $this->peek();

        if ($token === null)
        {
            throw new DBException(self::ERROR_INCOMPLETE);
        }

        if ($token->type === TokenType::Expression)
        {
            $this->position++;

            return new ComparisonNode($token->value);
        }

        if ($token->type === TokenType::OpeningParenthesis)
        {
            $this->position++;

            // Prázdné závorky nemají co vracet, přeskočíme je jako by tam nebyly.
            // Podmínka je tvoří i na kraji (`a = 1 AND ()`), tak je generují stavitelé
            // podmínek, když jim skupina filtrů vyjde prázdná.
            if ($this->peek()?->type === TokenType::ClosingParenthesis)
            {
                $this->position++;

                return $this->startsPrimary($this->peek()) ? $this->parsePrimary() : null;
            }

            $node = $this->parseOr();

            if ($this->peek()?->type !== TokenType::ClosingParenthesis)
            {
                throw new DBException(DB::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);
            }

            $this->position++;

            return $node;
        }

        throw new DBException(self::ERROR_INCOMPLETE);
    }


    /**
     * Přidá operand, pokud nějaký vznikl (prázdné závorky žádný nedávají).
     * @param list<Node> $operands
     */
    private function collect(array &$operands, ?Node $node): void
    {
        if ($node !== null)
        {
            $operands[] = $node;
        }
    }


    /**
     * Může tímto tokenem začínat výraz?
     */
    private function startsPrimary(?Token $token): bool
    {
        return $token !== null
            && ($token->type === TokenType::Expression || $token->type === TokenType::OpeningParenthesis);
    }


    /**
     * Byly v tomto úseku podmínky závorky?
     */
    private function spanHasParentheses(int $start, int $end): bool
    {
        for ($i = $start; $i < $end; $i++)
        {
            $type = $this->tokens[$i]->type;

            if ($type === TokenType::OpeningParenthesis || $type === TokenType::ClosingParenthesis)
            {
                return true;
            }
        }

        return false;
    }


    private function peek(): ?Token
    {
        return $this->tokens[$this->position] ?? null;
    }
}
