Changelog
=========

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


5.0.0 - 2026-08-26
----------

The next release is a major one - it contains changes that are not backwards compatible.
The `Upgrading` section of the README describes what to do about each of them.

### Added

+ `search()` returns a `SearchResult` object with the found records, the total number of
matches and the raw Elasticsearch response. It replaces passing the raw response through
the `$resultData` reference parameter of `findBy()`, which does not survive mocks and
decorators. `SearchResult` is iterable and offers `rows`, `total`, `isTotalExact()`,
`response`, `first()`, `isEmpty()` and `countRows()`.
+ `count()` returns the number of matching records as an `int`. Without `groupBy` it uses
the `_count` endpoint, with `groupBy` it counts the groups. `DBInterface` declares it, so
it is part of the unified API; `search()` is for now implemented by `ElasticsearchClient`
only.
+ Errors are reported through a [PSR-3](https://www.php-fig.org/psr/psr-3/) logger passed
to `setLogger()`. Without one the library logs to Tracy when `tracy/tracy` is installed
(the previous behaviour) and nowhere otherwise - Tracy became a `suggest` dependency
instead of a required one.
+ `where` conditions understand `NOT LIKE`, `IN`, `NOT IN`, an `ESCAPE 'x'` clause of
`LIKE`, and SQL literals in apostrophes (`name LIKE '%a AND b%'`).
+ All exceptions the library raises itself implement the `NoSQLException` marker
interface, so `catch (NoSQLException $e)` covers both `DBException` and
`NotImplementedException`.
+ A test suite (386 tests), including characterization tests that lock down the requests
the library builds, and PHPStan on level 8 over `src` and `tests`.

### Changed

+ Values passed to `where` are no longer stripped of "unsafe" characters and are never
inserted into the text of the condition. The text keeps a marker and the value travels
beside it, so a value may contain anything - `AND`, brackets, apostrophes, newlines - and
it keeps the type it was passed with (`30` stays an `int`, `'30'` stays a `string`).
+ A `where` condition may no longer contain the sequence `#number#`, which is reserved for
those markers.
+ `AND` and `OR` bind the way they do in SQL - `AND` first - even in conditions that mix
parentheses with unparenthesised connectives. The old parser cut the condition up as text
and got this wrong: `(name LIKE ?) AND active = ? OR admin = ?` used to be read as
`name LIKE ? AND (active = ? OR admin = ?)`.
+ `AND`/`OR` are connectives only where a space or a bracket separates them from their
surroundings, so `country IN [AND,FRA]` and `sku = ABC-AND-123` are single expressions.
+ A malformed condition (a dangling `AND`/`OR`) throws `DBException` instead of silently
building a nonsensical query. Empty parentheses are skipped wherever they appear, as they
were before - a condition made of nothing but empty parentheses still throws.
+ In an `OR` without parentheses each operand is wrapped in `bool.filter`, and in an `OR`
containing a parenthesis no operand is. The decision is made once for the whole `OR` so
that scored and unscored branches are not mixed. The same documents match either way, only
`_score` differs.
+ In a `LIKE` value, `*`, `?` and `\` are escaped and match themselves - they are not SQL
LIKE wildcards. Since values are no longer stripped, a user-typed `*` would otherwise
become an Elasticsearch wildcard and scan the whole index.
+ `count()` with `groupBy` returns the number of groups computed by a `cardinality`
aggregation (exact up to 40 000 distinct values). It used to count the returned `terms`
buckets, so it silently stopped at the requested bucket count, 100 by default.
+ A value that cannot be turned into text (an array, an object) throws `DBException` where
the condition needs text - `LIKE`, `CROSS FIELDS`, or a `?` surrounded by other text.
+ Only strings that are entirely a date or a date with time are converted to `DateTime`;
strings that merely contain something date-shaped are left alone.
+ `limit` and `offset` given as numeric strings are sent as numbers, and a non-numeric or
negative one is ignored instead of being rejected by Elasticsearch with a 400. Names of
aggregation columns are always sent as strings.
+ `deleteAll()` returns `void` instead of `true` - it throws on failure, so a plain call
is enough.
+ `DBInterface::get()`, `update()` and `delete()` declare `string|int $id` instead of
`int $id`, matching what `ElasticsearchClient` already accepted.
+ For descendants of `DBWithBooleanParsing`: `putValuesIntoQuery()` lost the
`$putPlaceholdersForDate` parameter and now binds every value, and the `parseAndOrQuery()`
hook is gone along with `trimAndOr()` and `parseAndArrayQuery()`. The condition is
tokenized and parsed into a tree (`Query\Parser`); descendants implement `addAndClause()`,
`addOrClause()` and `parseExpression()`, which are called for the nodes of that tree.
+ For descendants of `ElasticsearchClient`: the request is sent by `sendSearch()` and
`sendCount()`. Override those to reach the finished request - overriding `executeSearch()`
or `executeCount()` skips building it.
+ Internals were split out of `ElasticsearchClient` into `Query\FindByParams`,
`Query\OrderByField`, `Query\SearchRequestBuilder`, `Query\SearchResultMapper` and
`Type\DataTypeConverter`.

### Deprecated

+ The `$resultData` reference parameter of `findBy()`. Use `search()`, which returns the
raw response as part of `SearchResult`.

### Fixed

+ A value containing ` AND `, ` OR ` or brackets used to throw `No expression matched.` or
silently produce a wrong query. Such values now work.
+ `['name = ?' => '[not a list]']` used to be misread as a list of values.
+ A boolean `false` used to be sent as an empty string.
+ A `?` that is only a part of an expression (`name LIKE '%?%'`, `name = ?%`,
`id IN [?,?]`) has its value substituted into the text. In between it sent the internal
marker (`*#0#*`) to Elasticsearch and matched nothing.
+ A `DateTimeImmutable` in a `where` value is formatted like a `DateTime`. It used to be
sent as the serialized object, or to end the request with a fatal error.
+ A request to `_count` carries nothing but the query. It used to keep `from` and `aggs`
from the search request, which Elasticsearch rejects with a 400.
+ `SearchResult::isTotalExact()` tells whether `total` is a real number. Elasticsearch
stops counting at 10 000 matches by default and then reports `10000` with the relation
`gte`, which a pager would otherwise turn into a wrong number of pages.
+ A string that looks like a date but is not one (`2024-19-31`, `2024-02-30`) stays a
string. Reading such a value used to throw, or silently shift it to the next month.
+ Overriding `convertToDBDataTypes()`/`convertFromDBDataTypes()` applies to nested arrays
again, not just to the top level of the document.
+ `insertOrUpdate()`, `bulkInsertOrUpdate()`, `deleteAll()`, `createIndex()` and
`indexExists()` translate a missing index into `DBException` the same way the other
methods do.
+ A context value containing another key's `{placeholder}` is no longer substituted a
second time when logging through Tracy.
+ `IN`/`NOT IN` conditions are recognized last, so a `LIKE` or `CROSS FIELDS` value
containing the word `IN` no longer breaks the condition.


4.0.1 - 2026-08-06
------------------

+ Support for PHP 8.5.


4.0.0 - 2026-07-13
------------------

+ Updated to Elasticsearch 9.4.


3.1.0 - 2024-10-15
------------------

+ Fixed parsing of query parameters.
+ The raw Elasticsearch response can be read through the `$resultData` parameter of
`findBy()`.


3.0.4 - 2024-07-01
------------------

+ Support for PHP 8.3.


3.0.3 - 2024-03-06
------------------

+ `bulkInsertOrUpdate()` can wait for the data to be refreshed.


3.0.2 - 2024-02-28
------------------

+ The value of a wildcard (`LIKE`) filter can be modified through
`setWildcardValueFilter()`.


3.0.1 - 2024-02-08
------------------

+ The `size` parameter is no longer sent with a count query, which does not support it.


3.0.0 - 2023-12-19
------------------

+ PHP 8.2 or newer is required, the whole library uses `strict_types=1`.
+ The Elasticsearch index can be set per call.


2.0.1 - 2023-03-02
------------------

+ Installation allowed on PHP 8.2.


2.0.0 - 2021-09-24
------------------

+ Support for PHP 8 and Elasticsearch 7, types deprecated in Elasticsearch 7 removed.
+ `CROSS FIELDS` added as a search option.
+ HTTP authentication allowed, SSL verification can be disabled.
+ Characters that would break parsing were stripped from `where` values without warning.
+ Parameter names became constants on `DB`.
+ PHPStan added.


1.0.1 - 2018-02-14
------------------

+ Licence added, the version removed from `composer.json`.


1.0 - 2018-02-14
----------------

+ First release: a unified API over Elasticsearch with `insertOrUpdate()`,
`bulkInsertOrUpdate()`, `get()`, `update()`, `delete()`, `deleteAll()` and `findBy()`.
