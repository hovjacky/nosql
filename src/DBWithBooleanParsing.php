<?php

namespace Hovjacky\NoSQL;

use Tracy\Debugger;

abstract class DBWithBooleanParsing extends DB
{
    /**
     * Vloží hodnoty do dotazu místo `?`.
     *
     * @param string $condition
     * @param $values
     * @param boolean $putPlaceholdersForDate Mají se místo datumu vložit placeholdery? Nutné např. pro MongoDB.
     * @param array $placeholders Pole pro uložení placeholderů a k nim patřícím datumům.
     * @return string
     * @throws DBException
     */
    protected function putValuesIntoQuery($condition, $values, $putPlaceholdersForDate = false, &$placeholders = [])
    {
        $p = count($placeholders);
        if (isset($values))
        {
            $from = '/' . preg_quote('?', '/') . '/';
            foreach ($values as $value)
            {
                if (strpos($condition, '?') === false)
                {
                    Debugger::log("Too few questionmarks. Condition and values:", Debugger::ERROR);
                    Debugger::log($condition, Debugger::ERROR);
                    Debugger::log($values, Debugger::ERROR);
                    throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);
                }
                // Hodnotou může být i pole hodnot, převedeme jej do textové podoby
                if (is_array($value))
                {
                    $replace = '[' . implode(",", $value) . ']';
                }
                else
                {
                    $replace = $value;
                }

                if ($putPlaceholdersForDate && $value instanceof \DateTime)
                {
                    // Místo data dáme placeholder a datum uložíme do pole $placeholders
                    $replace = '#' . $p++ . '#';
                    $placeholders[$replace] = $value;
                }

                // Závorky nejsou v hodnotách povoleny, odstraníme je...
                $replace = preg_replace("/[^\p{L}\p{N}\-_@., :\+\[\]%]/u", "", $replace);

                $condition = preg_replace($from, $replace, $condition, 1);
            }
        }
        if (strpos($condition, '?') !== false)
        {
            Debugger::log("Too many questionmarks. Condition and values:", Debugger::ERROR);
            Debugger::log($condition, Debugger::ERROR);
            Debugger::log($values, Debugger::ERROR);
            throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);
        }
        return $condition;
    }

    /**
     * Rozparsuje booleovský dotaz pro Elasticsearch
     *
     * @param string $query
     * @return array
     * @throws DBException
     */
    protected function parseBooleanQuery($query)
    {
        $parNumber = substr_count($query, '(');
        if ($parNumber !== substr_count($query, ')'))
        {
            throw new DBException(self::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);
        }

        if ($parNumber > 0)
        {
            $elements = [];
            $pos = strpos($query, '(');
            while ($pos !== false)
            {
                // Něco je před závorkou
                if ($pos > 0)
                {
                    $temp = $this->trimAndOr(trim(substr($query, 0, $pos)));
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

                $temp = $this->trimAndOr(trim(substr($query, 0, $queryPosClose)));
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
                    $elements = array_merge($elements, $temp);
                }
            }

            $result = [];
            $iOld = 0;
            for ($i = 1; $i < count($elements); $i += 2)
            {
                // Rozdělíme podle OR
                if ($elements[$i] == 'OR')
                {
                    $this->addOrClause($result, $this->parseAndArrayQuery($elements, $iOld, $i));
                }
            }

            // A zpracujeme poslední část. Pokud je $result prázdný, je zbytečné tam dávat ['bool']['should'] (podobně v jiných případech)
            if (empty($result))
            {
                $result = $this->parseAndArrayQuery($elements, $iOld, count($elements));
            }
            else
            {
                $this->addOrClause($result, $this->parseAndArrayQuery($elements, $iOld, count($elements)));
            }
            return $result;
        }
        else
        {
            return $this->parseAndOrQuery($query);
        }

    }

    /**
     * Rozdělí dotaz na 2 části, pokud začíná nebo končí AND/OR
     *
     * @param string $query
     * @return array
     */
    protected function trimAndOr($query)
    {
        $result = [];
        if (strpos($query, 'AND') === 0 || strpos($query, 'OR') === 0)
        {
            $first = trim(substr($query, 0, 3));
            $second = trim(substr($query, 3));
        }
        elseif (strpos($query, 'AND') === mb_strlen($query) - 3 || strpos($query, 'OR') === mb_strlen($query) - 2)
        {
            $first = trim(substr($query, 0, mb_strlen($query) - 3));
            $second = trim(substr($query, mb_strlen($query) - 3));
        }
        else
        {
            $first = $query;
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
     * 'Rozparsuje' pole, každý sudý prvek je AND, každý lichý je dál parsován
     *
     * @param array $elements
     * @param int $start
     * @param int $end
     * @return array
     * @throws DBException
     */
    protected function parseAndArrayQuery($elements, &$start, $end)
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
     * Přidá $clause do $result jako and
     *
     * @param $result
     * @param $clause
     */
    protected abstract function addAndClause(&$result, $clause);

    /**
     * Přidá $clause do $result jako or
     *
     * @param $result
     * @param $clause
     */
    protected abstract function addOrClause(&$result, $clause);

    /**
     * Rozparsuje booleovský výraz bez závorek (jen AND a OR)
     *
     * @param $query
     * @return array
     */
    protected abstract function parseAndOrQuery($query);

    /**
     * Rozparsuje výraz proměnná >,<,=,>=,<=,!= hodnota
     *
     * @param $expr
     * @return mixed
     */
    protected abstract function parseExpression($expr);
}