<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\DB;
use Hovjacky\NoSQL\DBException;
use Hovjacky\NoSQL\Logging\TracyLogger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;

/**
 * Testy napojení knihovny na PSR-3 logger (dříve natvrdo Tracy\Debugger).
 */
final class LoggingTest extends TestCase
{
    private TestableElasticsearchClient $client;

    private RecordingLogger $logger;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
        $this->logger = new RecordingLogger();
        $this->client->setLogger($this->logger);
    }


    public function testTooFewPlaceholdersIsReportedToLogger(): void
    {
        try
        {
            $this->client->buildQuery('id = ?', [1, 2]);

            self::fail('Očekávána DBException.');
        }
        catch(DBException)
        {
            // Zpráva se posuzuje níž, tady jen potřebujeme, aby dotaz spadl.
        }

        self::assertCount(1, $this->logger->records);
        self::assertSame(LogLevel::ERROR, $this->logger->records[0]['level']);
        self::assertSame('Too few questionmarks in condition.', $this->logger->records[0]['message']);
        self::assertSame([1, 2], $this->logger->records[0]['context']['values']);
    }


    public function testTooManyPlaceholdersIsReportedToLogger(): void
    {
        try
        {
            $this->client->buildQuery('id = ? AND name = ?', [1]);

            self::fail('Očekávána DBException.');
        }
        catch(DBException)
        {
        }

        self::assertCount(1, $this->logger->records);
        self::assertSame(LogLevel::ERROR, $this->logger->records[0]['level']);
        self::assertSame('Too many questionmarks in condition.', $this->logger->records[0]['message']);
        // Do textu podmínky se vkládají značky, ne hodnoty.
        self::assertSame('id = #0# AND name = ?', $this->logger->records[0]['context']['condition']);
    }


    public function testSuccessfulQueryLogsNothing(): void
    {
        $this->client->buildQuery('id = ?', [1]);

        self::assertSame([], $this->logger->records);
    }


    public function testDefaultLoggerIsTracyWhenAvailable(): void
    {
        // V testech je Tracy nainstalovaná (require-dev), takže výchozí logger je adaptér nad ní.
        self::assertTrue(TracyLogger::isAvailable());
        self::assertInstanceOf(TracyLogger::class, (new TestableElasticsearchClient())->exposeLogger());
    }


    public function testExplicitLoggerWinsOverDefault(): void
    {
        self::assertSame($this->logger, $this->client->exposeLogger());
    }


    /**
     * Selhání logování nesmí zastínit chybu, kvůli které se loguje.
     * Tracy bez nastaveného adresáře pro logy vyhazuje LogicException.
     */
    public function testFailingLoggerDoesNotMaskTheRealException(): void
    {
        $client = new TestableElasticsearchClient();
        $client->setLogger(new ThrowingLogger());

        $this->expectException(DBException::class);
        $this->expectExceptionMessage(DB::ERROR_BOOLEAN_WRONG_NUMBER_OF_PLACEHOLDERS);

        $client->buildQuery('id = ?', [1, 2]);
    }
}
