<?php declare(strict_types=1);

namespace Hovjacky\NoSQL;

use DateTimeInterface;
use Tracy\Debugger;

/**
 * Class DBWithBooleanParsing
 * @package Hovjacky\NoSQL
 */
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
                    Debugger::log('Too few questionmarks. Condition and values: ', Debugger::ERROR);
                    Debugger::log($condition, Debugger::ERROR);
                    Debugger::log($values, Debugger::ERROR);

                    throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);
                }

                // Hodnotou může být i pole hodnot, převedeme jej do textové podoby
                if (is_array($value))
                {
                    $replace = '[' . implode(',', $value) . ']';
                }
                elseif (is_scalar($value))
                {
                    $replace = (string) $value;
                }

                if ($putPlaceholdersForDate && $value instanceof DateTimeInterface)
                {
                    // Místo data dáme placeholder a datum uložíme do pole $placeholders
                    $replace = '#' . $placeholdersCount++ . '#';
                    $placeholders[$replace] = $value;
                }

                if (!isset($replace) || !is_string($replace))
                {
                    throw new DBException('Hodnota filtru musí být převeditelná na textový řetězec');
                }

                // Závorky nejsou v hodnotách povoleny, odstraníme je...
                /** @var string $replace */
                $replace = preg_replace('/[^\p{L}\p{N}\-_@., :\+\[\]%]/u', '', $replace);

                $condition = (string) preg_replace($from, $replace, $condition, 1);
            }
        }

        if (str_contains($condition, '?'))
        {
            Debugger::log('Too many questionmarks. Condition and values:', Debugger::ERROR);
            Debugger::log($condition, Debugger::ERROR);
            Debugger::log($values, Debugger::ERROR);

            throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);
        }

        return $condition;
    }


    /**
     * Rozparsuje booleovský dotaz pro Elasticsearch.
     * @param string $query
     * @return array<string, mixed>
     * @throws DBException
     */
    protected function parseBooleanQuery(string $query): array
    {
        $parNumber = substr_count($query, '(');

        if ($parNumber !== substr_count($query, ')'))
        {
            throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);
        }

        if ($parNumber === 0)
        {
            return $this->parseAndOrQuery($query);
        }

        $elements = [];

        $pos = strpos($query, '(');

        while ($pos !== false)
        {
            // Něco je před závorkou
            if ($pos > 0)
            {
                $temp = $this->trimAndOr(trim(substr($query, 0, $pos)));

                /** @noinspection SlowArrayOperationsInLoopInspection */
                $elements = array_merge($elements, $temp);
            }

            $query = trim(substr($query, $pos + 1));

            // Zjistíme další pozici otevírací závorky a pozici uzavírací závorky
            $posOpen = strpos($query, '(');
            $posClose = strpos($query, ')');

            // Pozice hledané uzavírací závorky
            $queryPosClose = $posClose;

            $subQuery = $query;
            $openCount = 1;

            // Hledáme pozici uzavírací závorky, která uzavře původně nalezenou otevírací závorku ($pos)
            while ($openCount > 0)
            {
                if ($posOpen !== false && $posOpen < $posClose)
                {
                    $subQuery = (substr($subQuery, $posOpen + 1));
                    $posClose -= $posOpen + 1;
                    $posOpen = strpos($subQuery, '(');
                    $openCount++;
                }
                else
                {
                    $subQuery = (substr($subQuery, $posClose + 1));

                    if ($posOpen !== false)
                    {
                        $posOpen -= $posClose + 1;
                    }

                    $posClose = strpos($subQuery, ')');
                    $openCount--;

                    if ($openCount > 0)
                    {
                        $queryPosClose += $posClose + 1;
                    }
                }
            }

            $temp = $this->trimAndOr(trim(substr($query, 0, (int) $queryPosClose)));

            /** @noinspection SlowArrayOperationsInLoopInspection */
            $elements = array_merge($elements, $temp);

            // Pokud je to vše skončíme
            if ($posClose + 1 >= mb_strlen($query))
            {
                break;
            }

            // Jinak najdeme další pozici otevírací závorky nebo jen zbytek zpracujeme, pokud už další závorka není
            $query = trim($subQuery);
            $pos = strpos($query, '(');

            if ($pos === false && mb_strlen($query) > 0)
            {
                $temp = $this->trimAndOr(trim($query));

                /** @noinspection SlowArrayOperationsInLoopInspection */
                $elements = array_merge($elements, $temp);
            }
        }

        $result = [];
        $iOld = 0;

        for ($i = 1, $iMax = count($elements); $i < $iMax; $i += 2)
        {
            // Rozdělíme podle OR
            if ($elements[$i] === 'OR')
            {
                $this->addOrClause($result, $this->parseAndArrayQuery($elements, $iOld, $i));
            }
        }

        // A zpracujeme poslední část. Pokud je $result prázdný, je zbytečné tam dávat ['bool']['should'] (podobně v jiných případech)
        if (empty($result))
        {
            return $this->parseAndArrayQuery($elements, $iOld, count($elements));
        }

        $this->addOrClause($result, $this->parseAndArrayQuery($elements, $iOld, count($elements)));

        return $result;
    }


    /**
     * Rozdělí dotaz na 2 části, pokud začíná nebo končí AND/OR.
     * @param string $query
     * @return string[]
     */
    protected function trimAndOr(string $query): array
    {
        $result = [];
        $first = $query;
        $second = null;

        if (str_starts_with($query, 'AND') || str_starts_with($query, 'OR'))
        {
            $first = trim(substr($query, 0, 3));
            $second = trim(substr($query, 3));
        }
        elseif (strpos($query, 'AND') === mb_strlen($query) - 3 || strpos($query, 'OR') === mb_strlen($query) - 2)
        {
            $first = trim(substr($query, 0, mb_strlen($query) - 3));
            $second = trim(substr($query, mb_strlen($query) - 3));
        }

        if (!empty($first))
        {
            $result[] = $first;
        }

        if (!empty($second))
        {
            $result[] = $second;
        }

        return $result;
    }


    /**
     * 'Rozparsuje' pole, každý sudý prvek je AND, každý lichý je dál parsován.
     * @param string[] $elements
     * @param int $start
     * @param int $end
     * @return string[]
     * @throws DBException
     */
    protected function parseAndArrayQuery(array $elements, int &$start, int $end): array
    {
        $andResult = [];

        if ($start + 2 >= $end)
        {
            $andResult = $this->parseBooleanQuery($elements[$start]);
        }
        else
        {
            for ($j = $start; $j < $end; $j += 2)
            {
                $this->addAndClause($andResult, $this->parseBooleanQuery($elements[$j]));
            }
        }

        $start = $end + 1;

        return $andResult;
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
     * Rozparsuje booleovský výraz bez závorek (jen AND a OR).
     * @param string $query
     * @return array<string, mixed>
     */
    abstract protected function parseAndOrQuery(string $query): array;


    /**
     * Rozparsuje výraz proměnná >,<,=,>=,<=,!= hodnota.
     * @param string $expr
     * @return mixed
     */
    abstract protected function parseExpression(string $expr): mixed;
}
