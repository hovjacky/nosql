<?php declare(strict_types=1);

namespace Hovjacky\NoSQL\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Integrační test parsování složených podmínek tak, jak je generuje
 * SqlConditionBuilder z rtsoft/datagrid (zdroj dat datagrid-source-elastic).
 */
final class DatagridConditionsIntegrationTest extends TestCase
{
    private TestableElasticsearchClient $client;


    protected function setUp(): void
    {
        $this->client = new TestableElasticsearchClient();
    }


    public function testComposedDatagridConditions(): void
    {
        $query = $this->client->buildQuery(
            "(name LIKE ? ESCAPE '~') AND (age >= ? AND age < ?) AND (id IN ?)",
            ['%john%', 18, 65, [1, 2, 3]],
        );

        self::assertSame(
            [
                'bool' => [
                    'filter' => [
                        ['wildcard' => ['name' => '*john*']],
                        [
                            'bool' => [
                                'filter' => [
                                    ['bool' => ['filter' => [['range' => ['age' => ['gte' => 18]]]]]],
                                    ['bool' => ['filter' => [['range' => ['age' => ['lt' => 65]]]]]],
                                ],
                            ],
                        ],
                        ['terms' => ['id' => [1, 2, 3]]],
                    ],
                ],
            ],
            $query,
        );
    }


    public function testEmptyConditionAsGeneratedBySqlConditionBuilder(): void
    {
        // EmptyCondition z SqlConditionBuilder: `col IS NULL OR col LIKE ''`
        self::assertSame(
            [
                'bool' => [
                    'should' => [
                        ['bool' => ['filter' => [['bool' => ['must_not' => ['exists' => ['field' => 'street']]]]]]],
                        ['bool' => ['filter' => [['wildcard' => ['street' => '']]]]],
                    ],
                ],
            ],
            $this->client->buildQuery("street IS NULL OR street LIKE ''"),
        );
    }


    public function testNotEmptyConditionAsGeneratedBySqlConditionBuilder(): void
    {
        // NotEmptyCondition z SqlConditionBuilder: `col IS NOT NULL AND col NOT LIKE ''`
        self::assertSame(
            [
                'bool' => [
                    'filter' => [
                        ['exists' => ['field' => 'street']],
                        ['bool' => ['must_not' => ['wildcard' => ['street' => '']]]],
                    ],
                ],
            ],
            $this->client->buildQuery("street IS NOT NULL AND street NOT LIKE ''"),
        );
    }
}
