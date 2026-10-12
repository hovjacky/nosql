<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Query\Parser;

enum TokenType
{
    /** Text jednoho výrazu, např. `age >= 18`. Parser do něj dál nevidí, předá ho parseExpression(). */
    case Expression;

    case And_;

    case Or_;

    case OpeningParenthesis;

    case ClosingParenthesis;
}
