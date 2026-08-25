<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;

/**
 * Rozdělí text where podmínky na tokeny: závorky, spojky AND/OR a texty jednotlivých výrazů.
 *
 * Uvnitř SQL literálu v apostrofech (např. `name LIKE '%a AND b%'`) se závorky ani spojky
 * jako oddělovače neberou - literál je pro tokenizer jeden nedělitelný kus textu.
 */
final class Tokenizer
{
    private const KEYWORDS = [
        'AND' => TokenType::And_,
        'OR' => TokenType::Or_,
    ];


    /**
     * @return list<Token>
     * @throws DBException
     */
    public static function tokenize(string $query): array
    {
        $tokens = [];
        $buffer = '';
        $depth = 0;
        $length = strlen($query);
        $i = 0;

        while ($i < $length)
        {
            $char = $query[$i];

            if ($char === "'")
            {
                // Literál bereme včetně apostrofů a beze změny, ať už obsahuje cokoliv.
                $end = strpos($query, "'", $i + 1);
                $literalEnd = $end === false ? $length : $end + 1;
                $buffer .= substr($query, $i, $literalEnd - $i);
                $i = $literalEnd;

                continue;
            }

            if ($char === '(' || $char === ')')
            {
                self::flush($tokens, $buffer);

                if ($char === '(')
                {
                    $depth++;
                    $tokens[] = new Token(TokenType::OpeningParenthesis);
                }
                else
                {
                    if (--$depth < 0)
                    {
                        throw new DBException(DB::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);
                    }

                    $tokens[] = new Token(TokenType::ClosingParenthesis);
                }

                $i++;

                continue;
            }

            $keyword = self::keywordAt($query, $i);

            if ($keyword !== null)
            {
                self::flush($tokens, $buffer);

                $tokens[] = new Token(self::KEYWORDS[$keyword]);
                $i += strlen($keyword);

                continue;
            }

            $buffer .= $char;
            $i++;
        }

        if ($depth !== 0)
        {
            throw new DBException(DB::ERROR_BOOLEAN_WRONG_NUMBER_OF_PARENTHESES);
        }

        self::flush($tokens, $buffer);

        return $tokens;
    }


    /**
     * Vrátí klíčové slovo začínající na dané pozici, pokud tam samostatně stojí.
     */
    private static function keywordAt(string $query, int $position): ?string
    {
        if ($position > 0 && self::isWordCharacter($query[$position - 1]))
        {
            return null;
        }

        foreach (array_keys(self::KEYWORDS) as $keyword)
        {
            $end = $position + strlen($keyword);

            if (substr($query, $position, strlen($keyword)) !== $keyword)
            {
                continue;
            }

            if ($end < strlen($query) && self::isWordCharacter($query[$end]))
            {
                continue;
            }

            return $keyword;
        }

        return null;
    }


    private static function isWordCharacter(string $char): bool
    {
        // Bajty nad ASCII patří do vícebajtových znaků, tedy také do "slova".
        return $char === '_' || ctype_alnum($char) || ord($char) >= 0x80;
    }


    /**
     * @param list<Token> $tokens
     */
    private static function flush(array &$tokens, string &$buffer): void
    {
        $expression = trim($buffer);
        $buffer = '';

        if ($expression !== '')
        {
            $tokens[] = new Token(TokenType::Expression, $expression);
        }
    }
}
