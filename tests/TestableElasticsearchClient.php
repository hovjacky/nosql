<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\ElasticsearchClient;

/**
 * Testovací klient zpřístupňující sestavení Elasticsearch query z where podmínky
 * bez nutnosti připojení k Elasticsearch.
 */
final class TestableElasticsearchClient extends ElasticsearchClient
{
    public function __construct(array $params = [])
    {
        // Záměrně nevoláme parent konstruktor - testy nepotřebují připojení k Elasticsearch.
    }


    /**
     * Sestaví Elasticsearch query pro zadanou where podmínku a její hodnoty.
     * @param mixed[]|null $values
     * @return array<string, mixed>
     */
    public function buildQuery(string $condition, ?array $values = null): array
    {
        return $this->parseBooleanQuery($this->putValuesIntoQuery($condition, $values));
    }
}
