<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests\Logging;

use Hovjacky\NoSQL\Logging\TracyLogger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;
use RuntimeException;
use Tracy\Debugger;
use Tracy\ILogger;

/**
 * Testy PSR-3 adaptéru nad Tracy.
 */
final class TracyLoggerTest extends TestCase
{
    private RecordingTracyLogger $tracy;

    private TracyLogger $logger;

    private ILogger $originalTracyLogger;


    protected function setUp(): void
    {
        $this->originalTracyLogger = Debugger::getLogger();
        $this->tracy = new RecordingTracyLogger();
        Debugger::setLogger($this->tracy);

        $this->logger = new TracyLogger();
    }


    protected function tearDown(): void
    {
        Debugger::setLogger($this->originalTracyLogger);
    }


    /**
     * @return iterable<string, array{string, string}>
     */
    public static function levelProvider(): iterable
    {
        yield 'emergency' => [LogLevel::EMERGENCY, ILogger::CRITICAL];
        yield 'alert' => [LogLevel::ALERT, ILogger::CRITICAL];
        yield 'critical' => [LogLevel::CRITICAL, ILogger::CRITICAL];
        yield 'error' => [LogLevel::ERROR, ILogger::ERROR];
        yield 'warning' => [LogLevel::WARNING, ILogger::WARNING];
        yield 'notice' => [LogLevel::NOTICE, ILogger::INFO];
        yield 'info' => [LogLevel::INFO, ILogger::INFO];
        yield 'debug' => [LogLevel::DEBUG, ILogger::DEBUG];
    }


    #[\PHPUnit\Framework\Attributes\DataProvider('levelProvider')]
    public function testPsrLevelsMapToTracyLevels(string $psrLevel, string $tracyLevel): void
    {
        $this->logger->log($psrLevel, 'zpráva');

        self::assertSame($tracyLevel, $this->tracy->records[0]['level']);
    }


    public function testUnknownLevelFallsBackToInfo(): void
    {
        $this->logger->log('naprosto neznámá úroveň', 'zpráva');

        self::assertSame(ILogger::INFO, $this->tracy->records[0]['level']);
    }


    public function testPlaceholdersAreInterpolated(): void
    {
        $this->logger->error('Záznam {id} nenalezen v {index}.', ['id' => 42, 'index' => 'lidi']);

        self::assertSame('Záznam 42 nenalezen v lidi.', $this->tracy->records[0]['value']);
    }


    /**
     * Hodnota, která sama vypadá jako placeholder, se nesmí dosadit podruhé - jinak by
     * se do zprávy tiše propsala hodnota, kterou tam volající nedal.
     */
    public function testValueLookingLikeAPlaceholderIsNotSubstitutedAgain(): void
    {
        $this->logger->error('Dotaz {query}', ['query' => 'SELECT {secret}', 'secret' => 'hunter2']);

        self::assertSame("Dotaz SELECT {secret}\n  secret: hunter2", $this->tracy->records[0]['value']);
    }


    public function testContextWithoutPlaceholderIsAppended(): void
    {
        $this->logger->error('Neočekávaná odpověď.', ['status' => 500]);

        self::assertSame("Neočekávaná odpověď.\n  status: 500", $this->tracy->records[0]['value']);
    }


    public function testArrayContextIsDumpedReadably(): void
    {
        $this->logger->error('Chyba.', ['items' => ['a' => 1]]);

        $logged = $this->tracy->records[0]['value'];

        self::assertIsString($logged);
        self::assertStringStartsWith("Chyba.\n  items: ", $logged);
        self::assertStringContainsString('a', $logged);
        self::assertStringContainsString('1', $logged);
    }


    public function testExceptionInContextIsLoggedSeparatelyForTracy(): void
    {
        $exception = new RuntimeException('rozbité');

        $this->logger->error('Zápis selhal.', ['exception' => $exception, 'id' => 7]);

        self::assertCount(2, $this->tracy->records);
        // Výjimka se nesmí objevit ve zprávě, Tracy ji dostane samostatně (kvůli bluescreenu).
        self::assertSame("Zápis selhal.\n  id: 7", $this->tracy->records[0]['value']);
        self::assertSame($exception, $this->tracy->records[1]['value']);
        self::assertSame(ILogger::ERROR, $this->tracy->records[1]['level']);
    }


    public function testNonThrowableExceptionKeyIsTreatedAsOrdinaryContext(): void
    {
        $this->logger->error('Chyba.', ['exception' => 'jen text']);

        self::assertCount(1, $this->tracy->records);
        self::assertSame("Chyba.\n  exception: jen text", $this->tracy->records[0]['value']);
    }


    public function testMessageWithoutContextIsPassedThrough(): void
    {
        $this->logger->warning('Nic dalšího.');

        self::assertSame('Nic dalšího.', $this->tracy->records[0]['value']);
    }
}
