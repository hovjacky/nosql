<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use DateTimeInterface;
use Hovjacky\NoSQL\Query\Parser\Ast\AndNode;
use Hovjacky\NoSQL\Query\Parser\Ast\ComparisonNode;
use Hovjacky\NoSQL\Query\Parser\Ast\Node;
use Hovjacky\NoSQL\Query\Parser\Ast\OrNode;
use Hovjacky\NoSQL\Query\Parser\ConditionParser;
use Hovjacky\NoSQL\Query\Parser\Tokenizer;

abstract class DBWithBooleanParsing extends DB
{
    /**
     * Vloží hodnoty do dotazu místo `?`.
     * @param string $condition
     * @param mixed[]|null $values Seznam hodnot.
     * @param bool $putPlaceholdersForDate Mají se místo datumu vložit placeholdery? Nutné např. pro MongoDB.
     * @param array<string, mixed> $placeholders Pole pro uložení placeholderů a k nim patřícím datumům.
     * @return string
     * @throws DBException
     */
    protected function putValuesIntoQuery(
        string $condition,
        ?array $values,
        bool $putPlaceholdersForDate = false,
        array &$placeholders = [],
    ): string
    {
        $placeholdersCount = count($placeholders);

        if (isset($values))
        {
            $from = '/' . preg_quote('?', '/') . '/';

            foreach ($values as $value)
            {
                if (!str_contains($condition, '?'))
                {
                    $this->logError('Too few questionmarks in condition.', [
                        'condition' => $condition,
                        'values' => $values,
                    ]);

                    throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);
                }

                // Hodnotou může být i pole hodnot - do dotazu se vloží jako placeholder `[#n#]`
                // a skutečné hodnoty se předají bokem v poli $placeholders. Hodnoty seznamu tak
                // nepodléhají textové serializaci ani sanitizaci a zachovají si původní typy.
                if (is_array($value))
                {
                    $replace = '[#' . $placeholdersCount . '#]';
                    $placeholders['#' . $placeholdersCount++ . '#'] = $value;
                }
                elseif ($putPlaceholdersForDate && $value instanceof DateTimeInterface)
                {
                    // Místo data dáme placeholder a datum uložíme do pole $placeholders
                    $replace = '#' . $placeholdersCount++ . '#';
                    $placeholders[$replace] = $value;
                }
                elseif (is_scalar($value))
                {
                    // Závorky nejsou v hodnotách povoleny, odstraníme je...
                    // Znak `~` je povolen, protože se používá jako escape znak LIKE podmínek (klauzule ESCAPE).
                    $replace = (string) preg_replace('/[^\p{L}\p{N}\-_@., :\+\[\]%~]/u', '', (string) $value);
                }
                else
                {
                    throw new DBException('Hodnota filtru musí být převeditelná na textový řetězec');
                }

                $condition = (string) preg_replace($from, $replace, $condition, 1);
            }
        }

        if (str_contains($condition, '?'))
        {
            $this->logError('Too many questionmarks in condition.', [
                'condition' => $condition,
                'values' => $values,
            ]);

            throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);
        }

        return $condition;
    }


    /**
     * Rozparsuje booleovskou where podmínku a přeloží ji na dotaz konkrétní databáze.
     * @return array<string, mixed>
     * @throws DBException
     */
    protected function parseBooleanQuery(string $query): array
    {
        return $this->compileNode(ConditionParser::parse(Tokenizer::tokenize($query)));
    }


    /**
     * Přeloží uzel stromu na dotaz. AND/OR se skládají přes addAndClause()/addOrClause(),
     * jednotlivé výrazy rozebírá parseExpression().
     * @return array<string, mixed>
     * @throws DBException
     */
    private function compileNode(Node $node): array
    {
        if ($node instanceof ComparisonNode)
        {
            return $this->parseExpression($node->expression);
        }

        $result = [];

        if ($node instanceof AndNode)
        {
            foreach ($node->operands as $operand)
            {
                $this->addAndClause($result, $this->compileNode($operand));
            }

            return $result;
        }

        if (!$node instanceof OrNode)
        {
            throw new DBException('Neznámý uzel podmínky: ' . $node::class);
        }

        foreach ($node->operands as $operand)
        {
            $clause = $this->compileNode($operand);

            // Viz OrNode::$wrapOperands - kvůli zachování tvaru (a tím i skóre) dotazu.
            if ($node->wrapOperands && $operand instanceof ComparisonNode)
            {
                $wrapped = [];
                $this->addAndClause($wrapped, $clause);
                $clause = $wrapped;
            }

            $this->addOrClause($result, $clause);
        }

        return $result;
    }


    /**
     * Přidá $clause do $result jako and.
     * @param array<string, mixed> $result
     * @param array<string, mixed> $clause
     * @return void
     */
    abstract protected function addAndClause(array &$result, array $clause): void;


    /**
     * Přidá $clause do $result jako or.
     * @param array<string, mixed> $result
     * @param array<string, mixed> $clause
     * @return void
     */
    abstract protected function addOrClause(array &$result, array $clause): void;


    /**
     * Rozparsuje výraz proměnná >,<,=,>=,<=,!= hodnota.
     * @param string $expr
     * @return mixed
     */
    abstract protected function parseExpression(string $expr): mixed;
}
