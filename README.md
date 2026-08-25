NoSQL - PHP Clients
===================


This package creates unified API for some NoSQL databases (at the time only Elasticsearch,
MongoDB and Cassandra need to be finished).

Installation
------------
The recommended way is via Composer:
```
composer require hovjacky/nosql
```
It requires PHP 8.2 or higher and the `mbstring` extension.

Usage
-----

Connection parameters (host, port, index) have to be passed as an array to the constructor of
`ElasticsearchClient`.

The following methods are supported:

+ insertOrUpdate - array `$data`
+ bulkInsertOrUpdate - array `$data`
+ get - string|int `$id`
+ update - string|int `$id`, array `$data`
+ delete - string|int `$id`
+ deleteAll
+ findBy - array `$params` - details described below.
+ search - array `$params` - like findBy, but returns a `SearchResult` object.
+ count - array `$params` - number of matching records, always an `int`.

All have `table` as first parameter (in Elasticsearch it is the index name).

#### search and count

`findBy` returns either rows or a number depending on the `count` parameter. When you want
one specific thing, the typed methods are clearer:

```php
$result = $client->search('people', ['where' => ['age > ?' => 18], 'limit' => 10]);

$result->rows;        // array of matching records
$result->total;       // total number of matches reported by Elasticsearch (may be null)
$result->response;    // raw Elasticsearch response
$result->first();     // first record or null
foreach ($result as $row) { ... }

$howMany = $client->count('people', ['where' => ['age > ?' => 18]]);
```

`search` always returns records - a `count` parameter passed to it is ignored.

####Method findBy
Method findBy can be used for search. It supports quite a wide range of parameters
(you can find comparison with SQL in parentheses):

+ fields - array of fields user wants to retrieve (`SELECT id, name FROM`)
+ limit - max. number of results (`LIMIT 10`)
+ offset - can only be used with limit (`LIMIT 10, 10`)
+ count - only get number of results
+ orderBy - array of fields to order by, e.g. `['id', 'name']`, descendant order is marked by
lowercase desc - `['id desc', 'name']` (`ORDER BY id DESC, name`)
+ groupBy - **a single** field name to group by, e.g. `'name'` (`GROUP BY name`).
Grouping by several fields at once is not supported.
    + Elasticsearch only:
        + groupByScript - instead of a field name, `groupBy` can contain the word `script`
        and `groupByScript` can then contain an elasticsearch script
        (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html)
        + groupInternalOrderBy - array of fields to group by content inside each bucket
        (no SQL equivalent)
+ aggregation - an associative array, keys are aggregation functions like max, min, avg, etc.
and values are names of fields the aggregations should apply to, e.g.
`['min' => ['id', 'age'], 'avg' => ['age']]` (`SELECT MIN(id), MIN(age), AVG(age) FROM`)
+ where - an associative array, keys are conditions with placeholders (?) and values are values.
Conditions accept operators =, !=, >, <, >=, <=, LIKE, IS NULL, IS NOT NULL, CROSS FIELDS.
    + LIKE - the value should contain % (the same as in SQL)
    + = - the value can be an array, e.g. `['id = ?' => [1, 3, 7]]` (`id IN (1, 3, 7)`)
    + IS NULL, IS NOT NULL - Elasticsearch only
    + CROSS FIELDS - searching string in multiple fields, e.g. name `John Smith` in fields `firstname` and `surname`. Syntax is `"firstname,surname CROSS FIELDS ?" => "John Smith"`

Logging
-------

Errors the library detects internally (malformed `where` conditions, unexpected
Elasticsearch responses) are reported through a [PSR-3](https://www.php-fig.org/psr/psr-3/)
logger. Pass your own via `setLogger()`:

```php
$client = new ElasticsearchClient(['host' => 'localhost']);
$client->setLogger($myPsr3Logger);
```

If no logger is set, the library falls back to Tracy when `tracy/tracy` is installed
(preserving the previous behaviour), and logs nothing otherwise. Tracy is a `suggest`
dependency, no longer a required one - install it explicitly if you rely on that default.

All exceptions thrown by the library implement `Hovjacky\NoSQL\NoSQLException`, so
`catch (NoSQLException $e)` covers both `DBException` and `NotImplementedException`.

Upgrading
---------

Breaking changes that need a major version bump:

+ `deleteAll()` now returns `void` instead of `true`. Code doing `if ($db->deleteAll($t))`
must be rewritten - the method throws on failure, so a plain call is enough.
+ `DBInterface::get()`, `update()` and `delete()` now declare `string|int $id` instead of
`int $id`, matching what `ElasticsearchClient` already accepted. Implementations of the
interface that declare `int $id` must widen the type.
+ A malformed `where` condition (a dangling `AND`/`OR`, empty parentheses) now throws
`DBException`. It used to silently build a nonsensical query.
+ Strings that merely *contain* something date-shaped are no longer converted to `DateTime`;
only values that are entirely a date or a date with time are.

Deprecated, still working:

+ The `$resultData` reference parameter of `findBy()`. Use `search()`, which returns the raw
response as part of `SearchResult`.
