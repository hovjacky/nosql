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
     * Nahradí `?` v podmínce zástupnými značkami a skutečné hodnoty odloží bokem.
     *
     * Hodnoty se do textu dotazu nevkládají vůbec. Text tak obsahuje jen to, co napsal
     * vývojář, a hodnota nemůže rozbít parsování ani kdyby obsahovala `AND`, závorku
     * nebo apostrof. Zároveň se nemusí nijak ořezávat a zachovává si původní typ.
     *
     * @param mixed[]|null $values Seznam hodnot.
     * @param array<string, mixed> $boundValues Sem se uloží značka -> hodnota.
     * @throws DBException
     */
    protected function putValuesIntoQuery(string $condition, ?array $values, array &$boundValues): string
    {
        $index = count($boundValues);

        // Značky si generujeme sami, v zadané podmínce nemají co dělat - text napsaný
        // vývojářem by se jinak mohl vydávat za hodnotu. Bez hodnot žádné značky
        // nevznikají, takže tam není co zaměnit a kontrolovat se nemusí.
        if ($index === 0 && !empty($values) && preg_match('/#\d+#/', $condition) === 1)
        {
            throw new DBException(self::ERROR_BOOLEAN_RESERVED_SEQUENCE);
        }

        foreach ($values ?? [] as $value)
        {
            if (!str_contains($condition, '?'))
            {
                $this->logError('Too few questionmarks in condition.', [
                    'condition' => $condition,
                    'values' => $values,
                ]);

                throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);
            }

            if (!is_scalar($value) && !is_array($value) && !$value instanceof DateTimeInterface)
            {
                throw new DBException('Hodnotou filtru nemůže být ' . get_debug_type($value) . '.');
            }

            $token = self::valueToken($index++);
            $boundValues[$token] = $value;

            // Nahrazujeme značkou, ne hodnotou, takže se v ní nemůže nic interpretovat.
            $condition = (string) preg_replace('/\?/', $token, $condition, 1);
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
     * Značka zastupující hodnotu v textu podmínky.
     */
    protected static function valueToken(int $index): string
    {
        return '#' . $index . '#';
    }


    /**
     * Je tenhle text značkou zastupující hodnotu?
     */
    protected static function isValueToken(string $text): bool
    {
        return preg_match('/^#\d+#$/', $text) === 1;
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
