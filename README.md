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
It requires PHP version 7.0 and higher.

Usage
-----

Connection parameters (host, port, index) have to be passed as an array to the constructor of
`ElasticsearchClient`.

The following methods are supported:

+ insert - array `$data`
+ bulkInsert - array `$data`
+ get - int `$id`
+ update - int `$id`, array `$data`
+ delete - int `$id`
+ deleteAll
+ findBy - array `$params` - details described below.

All have `table` as first parameter (in Elasticsearch it is the type name).

####Method findBy
Method findBy can be used for search. It supports quite a wide range of parameters
(you can find comparison with SQL in parentheses):

+ fields - array of fields user wants to retrieve (`SELECT id, name FROM`)
+ limit - max. number of results (`LIMIT 10`)
+ offset - can only be used with limit (`LIMIT 10, 10`)
+ count - only get number of results
+ orderBy - array of fields to order by, e.g. `['id', 'name']`, descendant order is marked by
lowercase desc - `['id desc', 'name']` (`ORDER BY id DESC, name`)
+ groupBy - array of fields to group by, e.g. `['id', 'name']` (`GROUP BY id, name`)
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