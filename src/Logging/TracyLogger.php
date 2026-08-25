<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Logging;

use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;
use Stringable;
use Throwable;
use Tracy\Debugger;
use Tracy\Dumper;
use Tracy\ILogger;

/**
 * PSR-3 adaptér nad Tracy.
 *
 * Zachovává původní chování knihovny (chyby končí v Tracy logu), ale pouze jako
 * výchozí volbu - aplikace může přes DB::setLogger() předat jakýkoliv PSR-3 logger.
 * Pokud Tracy není nainstalovaná, knihovna tento adaptér nepoužije, viz isAvailable().
 */
final class TracyLogger extends AbstractLogger
{
    /** @var array<string, string> převod PSR-3 úrovní na úrovně Tracy */
    private const LEVELS = [
        LogLevel::EMERGENCY => ILogger::CRITICAL,
        LogLevel::ALERT => ILogger::CRITICAL,
        LogLevel::CRITICAL => ILogger::CRITICAL,
        LogLevel::ERROR => ILogger::ERROR,
        LogLevel::WARNING => ILogger::WARNING,
        LogLevel::NOTICE => ILogger::INFO,
        LogLevel::INFO => ILogger::INFO,
        LogLevel::DEBUG => ILogger::DEBUG,
    ];


    /**
     * Je Tracy vůbec k dispozici? Je jen doporučenou (`suggest`), ne povinnou závislostí.
     */
    public static function isAvailable(): bool
    {
        return class_exists(Debugger::class);
    }


    /**
     * @param mixed $level
     * @param mixed[] $context
     */
    public function log($level, string|Stringable $message, array $context = []): void
    {
        $tracyLevel = self::LEVELS[(string) $level] ?? ILogger::INFO;

        // Podle PSR-3 je pod klíčem `exception` výjimka - tu umí Tracy zalogovat
        // včetně bluescreenu, takže ji předáme zvlášť a ne jako text.
        $exception = $context['exception'] ?? null;

        if ($exception instanceof Throwable)
        {
            unset($context['exception']);
        }

        Debugger::log(self::format((string) $message, $context), $tracyLevel);

        if ($exception instanceof Throwable)
        {
            Debugger::log($exception, $tracyLevel);
        }
    }


    /**
     * Doplní do zprávy PSR-3 placeholdery `{klíč}`. Hodnoty, pro které zpráva
     * placeholder nemá, připojí čitelně za ni, aby se žádný kontext neztratil.
     * @param mixed[] $context
     */
    private static function format(string $message, array $context): string
    {
        foreach ($context as $key => $value)
        {
            $placeholder = '{' . $key . '}';

            if (str_contains($message, $placeholder))
            {
                $message = str_replace($placeholder, self::valueToString($value), $message);
            }
            else
            {
                $message .= "\n  " . $key . ': ' . self::valueToString($value);
            }
        }

        return $message;
    }


    private static function valueToString(mixed $value): string
    {
        if (is_scalar($value) || $value instanceof Stringable)
        {
            return (string) $value;
        }

        return $value === null ? 'null' : Dumper::toText($value);
    }
}
