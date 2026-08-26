<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use DateTime;
use Hovjacky\NoSQL\ElasticsearchClient;
use PHPUnit\Framework\TestCase;

/**
 * Testy převodu datových typů na klientovi.
 *
 * convertToDBDataTypes()/convertFromDBDataTypes() jsou součástí veřejného API a dají se
 * v potomkovi přepsat. Zanořování do vnořených polí proto musí jít přes `$this`, jinak by
 * se přepsání uplatnilo jen na první úrovni dokumentu.
 */
final class ConvertDataTypesTest extends TestCase
{
    /**
     * Klient, který navíc všechny texty převádí na velká písmena.
     */
    private static function shoutingClient(): ElasticsearchClient
    {
        return new class([]) extends ElasticsearchClient {
            /** @phpstan-ignore constructor.unusedParameter (signatura musí odpovídat předkovi) */
            public function __construct(array $params)
            {
                // Testy nepotřebují připojení k Elasticsearch.
            }


            public function convertToDBDataTypes(array $data): array
            {
                return array_map(
                    static fn (mixed $value): mixed => is_string($value) ? mb_strtoupper($value) : $value,
                    parent::convertToDBDataTypes($data),
                );
            }


            public function convertFromDBDataTypes(array $data): array
            {
                return array_map(
                    static fn (mixed $value): mixed => is_string($value) ? mb_strtolower($value) : $value,
                    parent::convertFromDBDataTypes($data),
                );
            }
        };
    }


    public function testOverriddenConversionAppliesToNestedArraysToo(): void
    {
        self::assertSame(
            ['a' => 'X', 'sub' => ['b' => 'Y', 'deep' => ['c' => 'Z']]],
            self::shoutingClient()->convertToDBDataTypes(['a' => 'x', 'sub' => ['b' => 'y', 'deep' => ['c' => 'z']]]),
        );
    }


    public function testOverriddenConversionFromDatabaseAppliesToNestedArraysToo(): void
    {
        self::assertSame(
            ['a' => 'x', 'sub' => ['b' => 'y', 'deep' => ['c' => 'z']]],
            self::shoutingClient()->convertFromDBDataTypes(['a' => 'X', 'sub' => ['b' => 'Y', 'deep' => ['c' => 'Z']]]),
        );
    }


    public function testDefaultConversionStillHandlesNestedDates(): void
    {
        $client = new TestableElasticsearchClient();

        self::assertSame(
            ['sub' => ['created' => '2024-01-31T12:00:00+00:00']],
            $client->convertToDBDataTypes(['sub' => ['created' => new DateTime('2024-01-31T12:00:00+00:00')]]),
        );

        $back = $client->convertFromDBDataTypes(['sub' => ['created' => '2024-01-31T12:00:00+00:00']]);

        self::assertInstanceOf(DateTime::class, $back['sub']['created']);
    }
}
