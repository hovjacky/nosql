NoSQL - PHP Clients
==============================================


This package creates unified API for some NoSQL databases (at the time only Elasticsearch, MongoDB and Cassandra need to be finished).

Installation
------------
The recommended way is via Composer:
```
composer require hovjacky/nosql
```
It requires PHP version 7.0 and higher.

Usage
-----

Connection parameters (host, port, index) have to be passed as an array to the constructor of `ElasticsearchClient`.

The following methods are supported:

+ insert
+ bulkInsert
+ get
+ update
+ delete
+ deleteAll
+ findBy

All have `table` as first parameter (in Elasticsearch it is type name).