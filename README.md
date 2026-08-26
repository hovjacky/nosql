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

Changes between releases are listed in [CHANGELOG.md](CHANGELOG.md).

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

$result->rows;            // array of matching records
$result->total;           // total number of matches reported by Elasticsearch (may be null)
$result->isTotalExact();  // false when Elasticsearch only reported a lower bound
$result->response;        // raw Elasticsearch response
$result->first();         // first record or null
foreach ($result as $row) { ... }

$howMany = $client->count('people', ['where' => ['age > ?' => 18]]);
```

`search` always returns records - a `count` parameter passed to it is ignored.

`total` comes from Elasticsearch, which by default stops counting at 10 000 matches and
then reports `10000` with the relation `gte`. `isTotalExact()` tells the two apart, so a
pager can avoid computing page counts from a number that is only a lower bound. `count()`
is always exact - it uses the `_count` endpoint, which has no such cap.

With `groupBy`, `search()->rows` are the groups but `total` is the number of documents they
were built from. The number of groups is what `count()` returns; it is computed with a
`cardinality` aggregation (exact up to 40 000 distinct values, an estimate above that),
because a `terms` aggregation only ever returns as many buckets as were asked for.

`search()` is currently implemented by `ElasticsearchClient` only, so it is not part of
`DBInterface`; `findBy()` and `count()` are.

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
Values are never inserted into the condition text - the text keeps a marker and the value is
passed alongside it. A value can therefore contain anything (`AND`, brackets, quotes, newlines)
without breaking the query, and it keeps the type you passed: `['age = ?' => 30]` sends the
number `30`, `['age = ?' => '30']` sends the string `"30"`. Values written directly into the
condition (`['age = 30' => null]`) have no type, so they are still guessed from the notation.
Conditions accept operators =, !=, >, <, >=, <=, LIKE, IS NULL, IS NOT NULL, CROSS FIELDS.
    + LIKE - the value should contain % (the same as in SQL). `_` matches a single
    character only with an `ESCAPE 'x'` clause. `*`, `?` and `\` are not wildcards
    in SQL LIKE, so they are escaped and match themselves.
    + = - the value can be a list, e.g. `['id = ?' => [[1, 3, 7]]]` (`id IN (1, 3, 7)`).
    The outer array holds one value per `?`, the inner one is the list itself - `['id = ?' => [1, 3, 7]]`
    would be three values for a single `?` and throws.
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

Errors the library detects itself are thrown as `DBException` or `NotImplementedException`,
both of which implement the `Hovjacky\NoSQL\NoSQLException` marker interface, so
`catch (NoSQLException $e)` covers them. Errors reported by Elasticsearch are rethrown as
the client's own exceptions, with one translation: a missing index becomes a `DBException`
with the message `DB::ERROR_DB_DOESNT_EXIST`.

Upgrading
---------

Breaking changes that need a major version bump:

+ Values passed to `where` are no longer stripped of "unsafe" characters. Previously
`['name = ?' => "J(o)h<n>='"]` searched for `John`; now it searches for the whole value.
If you relied on that stripping, sanitize the value yourself before passing it.
+ Values keep the type you pass them. A numeric string used to be turned into a number
(`'30'` became `30`); it now stays `'30'`. Elasticsearch coerces either way, so this only
matters if you inspect the generated query.
+ A `where` condition may no longer contain the sequence `#number#`, which is reserved for
value markers. Conditions without values are unaffected.
+ `deleteAll()` now returns `void` instead of `true`. Code doing `if ($db->deleteAll($t))`
must be rewritten - the method throws on failure, so a plain call is enough.
+ `DBInterface::get()`, `update()` and `delete()` now declare `string|int $id` instead of
`int $id`, matching what `ElasticsearchClient` already accepted. Implementations of the
interface that declare `int $id` must widen the type.
+ `DBInterface` now declares `count()`, which `DB` has implemented all along. Implementations
that extend `DB` need no change; anything implementing the interface directly must add it.
+ A malformed `where` condition (a dangling `AND`/`OR`) now throws `DBException`. It used
to silently build a nonsensical query. Empty parentheses are the exception: wherever they
appear (`a = ? AND ()`), they are skipped as if they were not written - that is the shape
a condition builder produces when a group of filters comes out empty. A condition made of
nothing but empty parentheses still throws.
+ `AND` and `OR` bind the way they do in SQL - `AND` first - even in conditions that mix
parentheses with unparenthesised connectives. The old parser cut the condition up as text
and got this wrong: `(name LIKE ?) AND active = ? OR admin = ?` used to be read as
`name LIKE ? AND (active = ? OR admin = ?)` and is now `(name LIKE ? AND active = ?) OR admin = ?`.
Fully parenthesised and fully unparenthesised conditions are unaffected. **If you rely on
the old grouping, add the parentheses explicitly.**
+ `AND`/`OR` are recognised as connectives only where a space or a bracket separates them
from their surroundings. `country IN [AND,FRA]` or `sku = ABC-AND-123` is one expression
again, as it was before the parser was rewritten.
+ In an `OR` without parentheses each operand is still wrapped in `bool.filter`; as soon as
the `OR` contains a parenthesis, no operand is wrapped. The decision is made once for the
whole `OR`, so that scored and unscored branches are not mixed. The same documents match
either way, only `_score` (and therefore the order of equally sorted results) differs.
+ In a `LIKE` value, `*`, `?` and `\` are escaped and match themselves. They used to be
stripped from bound values, and since values are no longer stripped they would otherwise
become Elasticsearch wildcards - a user-typed `*` would scan the whole index.
+ `limit` and `offset` given as numeric strings are sent as numbers (`'10'` -> `10`), a
non-numeric `offset` is ignored instead of being sent verbatim, and a negative `limit` or
`offset` is ignored instead of being rejected by Elasticsearch with a 400.
+ Names of aggregation columns are always sent as strings (`['min' => [5]]` sends `"5"`).
+ `count()` with `groupBy` returns the number of groups computed by a `cardinality`
aggregation. It used to count the returned `terms` buckets, so it silently stopped at the
requested bucket count (100 by default).
+ A value that cannot be turned into text (an array, an object) now throws `DBException`
when the condition needs text - `LIKE`, `CROSS FIELDS`, or a `?` surrounded by other text.
It used to emit `Array to string conversion` and search for the word `Array`.
+ Strings that merely *contain* something date-shaped are no longer converted to `DateTime`;
only values that are entirely a date or a date with time are. A string that looks like a
date but is not one (`2024-19-31`, `2024-02-30`) stays a string - reading it used to throw,
or silently shift the value to the next month.

Deprecated, still working:

+ The `$resultData` reference parameter of `findBy()`. Use `search()`, which returns the raw
response as part of `SearchResult`.

Fixed along the way:

+ A value containing ` AND `, ` OR ` or brackets used to throw `No expression matched.` or
silently produce a wrong query. Such values now work.
+ `['name = ?' => '[not a list]']` used to be misread as a list of values; it is now a string.
+ A boolean `false` used to be sent as an empty string.
+ A `?` that is only a part of the expression (`name LIKE '%?%'`, `name = ?%`, `id IN [?,?]`)
gets its value substituted into the text again. In between it sent the internal value marker
(`*#0#*`) to Elasticsearch and matched nothing.
+ A `DateTimeImmutable` in a `where` value is formatted like a `DateTime`. It used to be sent
as the serialized object, or to end the request with a fatal error.
+ Overriding `convertToDBDataTypes()`/`convertFromDBDataTypes()` applies to nested arrays
again, not just to the top level of the document.
+ A context value containing another key's `{placeholder}` is no longer substituted a second
time when the library logs through Tracy.

If you extend `DBWithBooleanParsing` yourself, note that:

+ `putValuesIntoQuery()` changed signature - it no longer takes `$putPlaceholdersForDate`
and always binds every value:
`putValuesIntoQuery(string $condition, ?array $values, array &$boundValues): string`.
+ the `parseAndOrQuery()` hook is gone, along with the `trimAndOr()` and
`parseAndArrayQuery()` helpers. The condition is now tokenized and parsed into a tree
(`Query\Parser`), and descendants only implement `addAndClause()`, `addOrClause()` and
`parseExpression()`, which are called for the nodes of that tree. An implementation that
overrode `parseAndOrQuery()` no longer takes part in the translation at all.

If you extend `ElasticsearchClient`, `executeSearch()`/`executeCount()` still exist but the
request is now sent by `sendSearch()`/`sendCount()`. Override those to get at the finished
request (a test double, a decorator that logs queries); overriding the `execute*` methods
skips building the request altogether.
