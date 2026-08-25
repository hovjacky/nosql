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
 */
final class ConditionParser
{
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

        return $node;
    }


    /**
     * @throws DBException
     * @phpstan-impure posouvá $this->position
     */
    private function parseOr(): Node
    {
        $start = $this->position;
        $operands = [$this->parseAnd()];

        while ($this->peek()?->type === TokenType::Or_)
        {
            $this->position++;
            $operands[] = $this->parseAnd();
        }

        if (count($operands) === 1)
        {
            return $operands[0];
        }

        return new OrNode($operands, !$this->spanHasParentheses($start, $this->position));
    }


    /**
     * @throws DBException
     * @phpstan-impure posouvá $this->position
     */
    private function parseAnd(): Node
    {
        $operands = [$this->parsePrimary()];

        while ($this->peek()?->type === TokenType::And_)
        {
            $this->position++;
            $operands[] = $this->parsePrimary();
        }

        return count($operands) === 1 ? $operands[0] : new AndNode($operands);
    }


    /**
     * @throws DBException
     * @phpstan-impure posouvá $this->position
     */
    private function parsePrimary(): Node
    {
        $token = $this->peek();

        if ($token === null)
        {
            throw new DBException('Neúplná podmínka - očekáván výraz.');
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
            if ($this->peek()?->type === TokenType::ClosingParenthesis)
            {
                $this->position++;

                return $this->parsePrimary();
            }

            $node = $this->parseOr();

            if ($this->peek()?->type !== TokenType::ClosingParenthesis)
            {
                throw new DBException(DB::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);
            }

            $this->position++;

            return $node;
        }

        throw new DBException('Neúplná podmínka - očekáván výraz.');
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
