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
     * Jde přesně tou cestou, kterou dotaz staví findBy(), včetně převodu hodnot.
     * @param mixed[]|null $values
     * @return array<string, mixed>
     */
    public function buildQuery(string $condition, ?array $values = null): array
    {
        return $this->compileWhereCondition($condition, $values);
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


    /** @var mixed[] odpověď, kterou klient „dostane“ z Elasticsearch */
    public array $fakeResponse = [];

    public int $fakeCount = 0;

    /** @var array<string, mixed>|null poslední odeslaný dotaz */
    public ?array $lastRequest = null;


    /**
     * Odpověď se podstrčí až na hranici odesílání, aby se dotaz opravdu sestavil
     * a testy viděly to, co by šlo do Elasticsearch.
     * @param array<string, mixed> $request
     * @return mixed[]
     */
    protected function sendSearch(array $request): array
    {
        $this->lastRequest = $request;

        return $this->fakeResponse;
    }


    /**
     * @param array<string, mixed> $request
     */
    protected function sendCount(array $request): int
    {
        $this->lastRequest = $request;

        return $this->fakeCount;
    }
}
