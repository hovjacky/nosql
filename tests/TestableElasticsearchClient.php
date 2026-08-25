<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use Hovjacky\NoSQL\ElasticsearchClient;
use Psr\Log\LoggerInterface;

/**
 * Testovací klient zpřístupňující sestavení Elasticsearch query z where podmínky
 * bez nutnosti připojení k Elasticsearch.
 */
final class TestableElasticsearchClient extends ElasticsearchClient
{
    /** @phpstan-ignore constructor.unusedParameter (signatura musí odpovídat předkovi) */
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
        return $this->parseWhereCondition($condition, $values);
    }


    /**
     * Zpřístupní logger, který klient reálně používá (výchozí nebo nastavený přes setLogger()).
     */
    public function exposeLogger(): LoggerInterface
    {
        return $this->getLogger();
    }


    /**
     * Zpřístupní kontrolu a normalizaci parametrů findBy().
     * @param array<string, mixed> $params
     * @return array<string, mixed>
     */
    public function repairParams(array $params): array
    {
        return $this->checkAndRepairParams($params);
    }
}
