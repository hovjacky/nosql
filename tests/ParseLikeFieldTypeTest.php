<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Testy překladu LIKE podle typu pole (enableFieldTypeDetection()).
 *
 * Textové pole je v indexu rozložené na termy, wildcard tedy porovnává vzor s jednotlivými
 * slovy, ne s celou hodnotou. „Začíná na“ a víceslovné „obsahuje“ se proto překládají
 * na intervals query nad posloupností termů.
 */
final class ParseLikeFieldTypeTest extends TestCase
{
    private const ANCHOR = ['script' => ['source' => 'interval.start == 0']];

    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
        $this->client->fakeFieldTypes = [
            'name' => 'text',
            'customer.name' => 'text',
            'email' => 'keyword',
            'age' => 'long',
        ];
        $this->client->enableFieldTypeDetection();
        $this->client->setWildcardValueFilter(static fn (string $value): string => 'filtered:' . $value);
        $this->client->fakeResponse = ['hits' => ['hits' => []]];
    }


    /**
     * Přeloží podmínku cestou search(), tedy se známým indexem.
     * @param mixed[] $values
     * @return mixed
     */
    private function query(string $condition, array $values): mixed
    {
        $this->client->search('index', ['where' => [$condition => $values]]);

        return $this->client->lastRequest['body']['query'] ?? null;
    }


    /**
     * @return iterable<string, array{string, list<array<string, mixed>>}>
     */
    public static function startsWithProvider(): iterable
    {
        yield 'jedno slovo' => ['Jiří%', [['prefix' => ['prefix' => 'Jiří']]]];
        yield 'interpunkce na konci' => ['Ing.%', [['prefix' => ['prefix' => 'Ing']]]];
        yield 'více slov' => ['Ing. Jiří Ra%', [
            ['match' => ['query' => 'Ing. Jiří', 'ordered' => true, 'max_gaps' => 0]],
            ['prefix' => ['prefix' => 'Ra']],
        ]];
        yield 'mezera na konci = celé slovo' => ['Jiří %', [
            ['match' => ['query' => 'Jiří', 'ordered' => true, 'max_gaps' => 0]],
        ]];
        yield 'pomlčka dělí term' => ['Novák-Sv%', [
            ['match' => ['query' => 'Novák-', 'ordered' => true, 'max_gaps' => 0]],
            ['prefix' => ['prefix' => 'Sv']],
        ]];
        yield 'tečka uvnitř termu' => ['s.r%', [['prefix' => ['prefix' => 's.r']]]];
    }


    /**
     * @param list<array<string, mixed>> $intervals
     */
    #[DataProvider('startsWithProvider')]
    public function testStartsWithIsAnchoredToFirstTerm(string $pattern, array $intervals): void
    {
        self::assertSame(
            ['intervals' => ['name' => ['all_of' => [
                'intervals' => $intervals,
                'ordered' => true,
                'max_gaps' => 0,
                'filter' => self::ANCHOR,
            ]]]],
            $this->query('name LIKE ?', [$pattern]),
        );
    }


    public function testMultiWordContainsSearchesSequenceOfTerms(): void
    {
        self::assertSame(
            ['intervals' => ['name' => ['all_of' => [
                'intervals' => [
                    ['match' => ['query' => 'Jiří', 'ordered' => true, 'max_gaps' => 0]],
                    ['prefix' => ['prefix' => 'Ra']],
                ],
                'ordered' => true,
                'max_gaps' => 0,
            ]]]],
            $this->query('name LIKE ?', ['%Jiří Ra%']),
        );
    }


    /**
     * Jednoslovné „obsahuje“ zůstává na wildcard nad termy (včetně filtru hodnoty).
     */
    public function testSingleWordContainsStaysWildcard(): void
    {
        self::assertSame(
            ['wildcard' => ['name' => 'filtered:*iří*']],
            $this->query('name LIKE ?', ['%iří%']),
        );
    }


    /**
     * @return iterable<string, array{string}>
     */
    public static function untranslatablePatternProvider(): iterable
    {
        yield 'končí na' => ['%ner'];
        yield 'bez zástupných znaků' => ['Jiří'];
        yield 'zástupný znak uprostřed' => ['Ji%ner'];
        yield 'jen zástupný znak' => ['%'];
    }


    /**
     * Vzory, které nad textovým polem správně přeložit nejde, zůstávají postaru.
     */
    #[DataProvider('untranslatablePatternProvider')]
    public function testUntranslatablePatternsStayWildcard(string $pattern): void
    {
        $query = $this->query('name LIKE ?', [$pattern]);

        self::assertIsArray($query);
        self::assertArrayHasKey('wildcard', $query);
    }


    public function testNotLikeNegatesIntervalsQuery(): void
    {
        $query = $this->query('name NOT LIKE ?', ['Jiří%']);

        self::assertIsArray($query);
        self::assertSame(['bool', 'must_not', 'intervals'], [
            array_key_first($query),
            array_key_first($query['bool']),
            array_key_first($query['bool']['must_not']),
        ]);
    }


    public function testEscapedWildcardIsLiteral(): void
    {
        self::assertSame(
            ['intervals' => ['name' => ['all_of' => [
                'intervals' => [
                    ['match' => ['query' => '50%', 'ordered' => true, 'max_gaps' => 0]],
                    ['prefix' => ['prefix' => 'sl']],
                ],
                'ordered' => true,
                'max_gaps' => 0,
                'filter' => self::ANCHOR,
            ]]]],
            $this->query("name LIKE ? ESCAPE '~'", ['50~% sl%']),
        );
    }


    public function testNestedFieldPath(): void
    {
        $query = $this->query('customer.name LIKE ?', ['Jiří%']);

        self::assertIsArray($query);
        self::assertArrayHasKey('customer.name', $query['intervals']);
    }


    /**
     * Keyword se neanalyzuje - hodnota se nesmí upravit filtrem ani převést na malá písmena.
     */
    public function testKeywordFieldIsCaseInsensitiveWithoutValueFilter(): void
    {
        self::assertSame(
            ['wildcard' => ['email' => ['value' => '*Novák@*', 'case_insensitive' => true]]],
            $this->query('email LIKE ?', ['%Novák@%']),
        );
    }


    /**
     * Pole jiného typu nebo pole, které v mapování není, se překládá postaru.
     */
    public function testOtherFieldTypesKeepOriginalTranslation(): void
    {
        self::assertSame(['wildcard' => ['age' => 'filtered:']], $this->query('age LIKE ?', ['']));
        self::assertSame(['wildcard' => ['unknown' => 'filtered:jiří*']], $this->query('unknown LIKE ?', ['Jiří%']));
    }


    /**
     * Bez zapnutí rozlišování typů se mapování nenačítá a LIKE se překládá postaru.
     */
    public function testDetectionIsDisabledByDefault(): void
    {
        $client = new TestableElasticsearchClient();
        $client->fakeFieldTypes = ['name' => 'text'];
        $client->fakeResponse = ['hits' => ['hits' => []]];
        $client->search('index', ['where' => ['name LIKE ?' => ['Jiří%']]]);

        self::assertSame(['wildcard' => ['name' => 'jiří*']], $client->lastRequest['body']['query'] ?? null);
        self::assertSame(0, $client->fetchFieldTypesCount);
        self::assertNull($client->getFieldType('index', 'name'));
    }


    public function testMappingIsLoadedOncePerIndex(): void
    {
        $this->query('name LIKE ?', ['Jiří%']);
        $this->query('email LIKE ?', ['x%']);
        $this->client->count('index', ['where' => ['name LIKE ?' => ['Jiří%']]]);

        self::assertSame(1, $this->client->fetchFieldTypesCount);
    }


    /**
     * Count jde přes `_count` - i tam se musí typy polí uplatnit.
     */
    public function testCountUsesFieldTypes(): void
    {
        $this->client->count('index', ['where' => ['name LIKE ?' => ['Jiří%']]]);

        self::assertArrayHasKey('intervals', $this->client->lastRequest['body']['query'] ?? []);
    }


    /**
     * Bez známého indexu (přímé sestavení podmínky) se typy polí nepoužijí.
     */
    public function testWithoutKnownIndexTranslationIsUnchanged(): void
    {
        self::assertSame(['wildcard' => ['name' => 'filtered:jiří*']], $this->client->buildQuery('name LIKE ?', ['Jiří%']));
    }
}
