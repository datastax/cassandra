<!---
Copyright DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Index Hints

Index hints are user-provided directives about what indexes should be used by a `SELECT` query.
They consist of a set of indexes that should be used (included) and a set of indexes that should not be used (excluded). 
The CQL syntax is:
```
SELECT ... FROM ... WHERE ...
  WITH included_indexes = { ... } AND excluded_indexes = { ... };
```
So, for example:
```
CREATE TABLE users (
  username text PRIMARY KEY,
  birth_year int,
  country text,
  phone text
);

CREATE INDEX birth_year_idx ON users (birth_year);
CREATE INDEX country_idx ON users (country);
CREATE INDEX phone_idx ON users (phone);
```
The following query will use the index on `birth_year` and will not use the indexes on `country` and `phone`:
```
SELECT * FROM users
  WHERE birth_year = 1981 AND country = 'FR'
  ALLOW FILTERING
  WITH included_indexes = {birth_year_idx}
  AND excluded_indexes = {country_idx, phone_idx};
```
Please note that the query requires ALLOW FILTERING because there is a restriction on the country column, 
but we are explicitly excluding that index. 
Note also that excluding the index on phone is a no-op because there isn’t any restriction on it.

It’s guaranteed that the queries will utilize all the included indexes or fail if it’s not possible to do so. 
It will never happen that the query succeeds without using the included indexes. 
Queries might fail because the query doesn't have a restriction for those indexes, 
because there is a restriction that could use the index but is not compatible with other restrictions, 
or because the underlying index implementation isn't able to use the index for some reason.

Excluded indexes will never cause the query to fail unless they reference a non-existent index, 
since it’s always possible to exclude an index regardless of the query expressions and index implementation capabilities. 
However, excluding indexes might make it necessary to add ALLOW FILTERING to the query.

Other than these two sets of included and excluded indexes, 
indexes that are applicable to the query and that are not mentioned in these two sets might or might not be used, 
depending on the index query planner.

## Disambiguating queries

Index hints can also be used to disambiguate queries where a restricted column has multiple indexes that return 
different results. For example, we can have analyzed and not-analyzed indexes in the same column. An equality query on
that columns would throw an exception due to the ambiguity:
```
CREATE TABLE t(k int PRIMARY KEY, v text);
CREATE CUSTOM INDEX not_analyzed_idx ON t(v) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX analyzed_idx ON t(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' };
SELECT * FROM t WHERE v = '...'; # rejected query due to ambiguity
```
But if we add hints to prefer or exclude one of the indexes the query will work:
```
SELECT * FROM t WHERE v = '...' WITH included_indexes = {not_analyzed_idx};
SELECT * FROM t WHERE v = '...' WITH included_indexes = {analyzed_idx};
```
## Unshading queries

The presence of indexes can shade queries that used to have a different behaviour without indexes. 
The hints can be used to access the previous behaviour. 
For example, an analyzed index will shade `ALLOW FILTERING`'s full-value equality:
```
CREATE TABLE t(k int PRIMARY KEY, v text);
SELECT * FROM t WHERE v = '...' ALLOW FILTERING; # exact equality mathc
CREATE CUSTOM INDEX idx ON t(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' };
SELECT * FROM t WHERE v = '...' ALLOW FILTERING; # uses the analyzed index, shading the previous query
```
But we can use hints to exclude that index and get access to the not-indexed behaviour:
```
SELECT * FROM t WHERE v = '...' ALLOW FILTERING WITH excluded_indexes = {idx}; # uses not-analyzed filtering
```
The same type of shading happens with `[NOT] CONTAINS [KEY]`.

## Choosing between index implementations

Columns can have multiple indexes with different implementations.
The hints can be used to prefer or exclude specific implementations. 
For example, we can have a legacy index and a SAI index on the same column:
```
CREATE TABLE t(k int PRIMARY KEY, v text);
CREATE INDEX legacy_idx ON t(v);
CREATE CUSTOM INDEX sai_idx ON t(v) USING 'StorageAttachedIndex';
SELECT * FROM t WHERE v = '...'; # uses the SAI index
```
The index manager will always prefer the SAI index over the legacy index. 
However, we can use hints to prefer the legacy index:
```
SELECT * FROM t WHERE v = '...' WITH included_indexes = {legacy_idx}; # uses the legacy index
SELECT * FROM t WHERE v = '...' WITH excluded_indexes = {sai_idx}; # also uses the legacy index
```