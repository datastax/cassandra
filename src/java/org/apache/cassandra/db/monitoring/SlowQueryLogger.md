<!--
Copyright IBM Corp.

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

# Slow Query Logger

Slow query logger is a feature that tracks the slowest `SELECT` queries and periodically emits them in logs.
It emits separate log reports for slow but successful queries and queries aborted due to slowness.
It operates on a per-node basis, when the read commands are executed on the replicas.

The slow queries are logged in their CQL representation, with all filtering column values redacted.
Queries of the same form will be aggregated within the report window, 
showing only the number of times the query has been slow and the metrics for the slowest execution.

The feature is meant to help operators identify abnormally slow queries that might need some kind of improvement.
The logged reports include some metrics about the queries to help operators identify what's wrong with the query.

## Configuration

Queries are considered slow if their running time exceeds the `slow_query_log_timeout_in_ms` config property,
defined in `cassandra.yaml`. It defaults to 500 milliseconds. It can not be changed dynamically.

Also, there are the following JVM properties:
* `-Dcassandra.monitoring_report_interval_ms`: The interval for reporting any operations that have timed out. 
  That is, the frequency at which the log reports are emited. It defaults to 5000 milliseconds.
  It can not be changed dynamically.
* `-Dcassandra.monitoring_max_operations`: 
  The maximum number of slowest unique queries that will be tracked and reported in each log report. 
  There are separate counters for slow but successful and aborted queries.  
  A query hit multiple times during the interval between reports will only produce an entry in the log report. 
  Due to value redaction, queries of the same form but with different values will produce a single entry.
  This single entry will show the number of times the query has been slow and the metrics for the slowest execution.
  A negative value means no limit. It defaults to 50 queries per each of the slow and aborted queries queues.
  It can not be changed dynamically.
* `-Dcassandra.monitoring_execution_info_enabled`: 
  Whether to log detailed execution info when logging slow or aborted non-SAI queries. 
  If this is `false`, only a CQL approximate representation of the query, 
  the number of hits and metrics about the running time will be printed, taking less space in logs. 
  If the property is `true`, the reports will also include metrics about the number of fetched and returned partitions, 
  rows and tombstones, taking more space in logs. Defaults to true.
  It can be changed dynamically.
* `-Dcassandra.sai.monitoring_execution_info_enabled`: 
  Whether to log detailed execution info when logging slow or aborted SAI queries. 
  If this is `false`, only a CQL approximate representation of the query, 
  the number of hits and metrics about the running time will be printed, taking less space in logs. 
  If the property is `true`, the reports will also include internal index metrics, taking more space in logs.
  Those metrics include the number of fetched and returned index keys partitions, rows and tombstones, 
  index segments hits, ANN latency, the index query plan, planner metrics, etc.
  It can be changed dynamically.

## Representing internal commands as CQL queries

The slow query logger monitors the replica-side internal commands in which a user-provided CQL query is translated to. 
Once identified, it prints a CQL representation of those internal commands.
Unfortunately, it's not always possible to produce a CQL translation of the commands 
that is identical to the user-provided CQL query that produced those commands.
There are some caveats with the reverse translation that should be taken into account when trying to figure out 
what user CQL query produced a slow query logger entry when allow filtering or paging are used.

### Allow filtering

The replicas don't know if the original query used `ALLOW FILTERING`. 
The slow queries are arbitrarily printed with `ALLOW FILTERING`, 
but it doesn't mean that the original query used it.
Thus, the presence of `ALLOW FILTERING` in the slow query log reports should be ignored.

However, the detailed execution info for those queries can be used to achieve a similar purpose 
by comparing the number of rows fetched to the number of rows returned. 
If there is a difference, it's likely that `ALLOW FILTERING` was used.

### Paging

User queries can use paging, which splits the original CQL into multiple commands with a smaller `LIMIT`.
Paging also adds restrictions about the last seen primary key. 
Thus, seeing a `LIMIT` on a slow query report doesn't mean that the original query had that same `LIMIT`.
Restrictions regarding the primary key on the `WHERE` clause can indicate paging too.

A good hint to identify paging is that the `LIMIT` shown in the log reports will match the paging fetch size,
which in the case of the Java driver is 5000 rows by default. 
Also, if the slow query matches more that these rows and has to request multiple pages,
we would see versions of the same query with and without primary key restrictions.

## Execution info for non-SAI queries

The log reports for non-SAI `SELECT` queries include execution info details
when `-Dcassandra.monitoring_execution_info_enabled` is `true`.
The execution details can be used to help diagnose why the query is slow.

Enabling slow query execution details will produce larger log entries because of the addition information provided.
The log messages for slow queries can take up around 60% extra (uncompressed) disk space. 
In the worst of the worst cases, with all queries being permanently slower than 500ms, 
and at least 50 different queries, reporting every 5 seconds (the default), 
the slow query logger can produce around 100MB of text per day and writer, depending on the queries. W
ith the detailed logging, it would be 160MiB per day. 
That is for tiniest queries, for queries with more predicates, longer column names, 
and long keyspace names the difference would be smaller.

These slow query execution details consist on the following metrics:

* Number of fetched partitions, before applying filtering.
* Number of returned partitions, after applying filtering.
  If it is lower than the number of fetched partitions it means that filtering is used. 
  Much filtering has a high performance cost and maybe better modeling or indexing can help.
* Number of found partition tombstones (partition deletions).
  Many tombstones can have a significant performance cost.
* Number of fetched rows, before applying filtering.
  If there are many rows per partitions it means that there are large partitions.
  Partitions over a few tens of thousands rows are generally problematic for performance,
  and better modelling should be considered.
* Number of returned rows, after applying filtering.
  If it is lower than the number of fetched rows it means that filtering is used.
  Much filtering has a high performance cost and maybe better modeling or indexing can help.
* Number of found row tombstones (row deletions). 
  It counts both individual row tombstones and range tombstones as a single tombstone, 
  even if a range tombstone can mean the deletion of many rows.
  Many tombstones can have a significant performance cost.

Here is an example of how the execution details look like:
```
1 operations were slow in the last 5001 msecs:
<SELECT * FROM k.t WHERE k = ? AND v = ? ALLOW FILTERING>, time 507 msec - slow timeout 500 msec/cross-node
  Fetched/returned/tombstones:
    partitions: 1/1/0
    rows: 10/10/123456
```
It shows a CQL single-partition query. 
The execution info right below has indeed fetched and returned a single partition, as expected.
For that partition, it has fetched 10 rows and returned the same 10 rows, indicating that probably didn't use
`ALLOW FILTERING`, or at least that `ALLOW FILTERING` isn't an issue in this case.
However, it has found 123456 row tombstones, which might be the cause of it being slow.

Another example:
```
1 operations were slow in the last 5001 msecs:
<SELECT * FROM k.t WHERE k = ? AND v = ? ALLOW FILTERING>, time 507 msec - slow timeout 500 msec/cross-node
  Fetched/returned/tombstones:
    partitions: 1/1/0
    rows: 123456/10/0
```
It shows the same CQL single-partition query as before.
This time there are no tombstones.
However, it has fetched 123456 rows but returned only 10, indicating that it actively used `ALLOW FILTERING`,
which might be the cause of it being slow.

If multiple queries of the same form are slow during the reporting window defined by 
`-Dcassandra.monitoring_report_interval_ms`, they will be aggregated 
showing only the number of times the query has been slow and the metrics for the slowest execution.
Here is an example:
```
 1 operations were slow in the last 5001 msecs:
<SELECT * FROM k.t WHERE k = ? AND v = ? ALLOW FILTERING>, was slow 10 times: avg/min/max 205/203/207 msec - slow timeout 100 msec/cross-node
  Slowest fetched/returned/tombstones:
    partitions: 1/1/0
    rows: 123456/10/0
```
It shows the same query being slow 10 times, together with the average, minimum and maximum execution times. 
Only the metrics of the slowest query execution are shown.
Unfortunately, due to redaction we cannot know whether the 10 queries used the same values or not.

## Execution info for SAI queries

The log reports for SAI `SELECT` queries include execution info details
when `-Dcassandra.sai.monitoring_execution_info_enabled` is `true`.
These details are the index query metrics and the index query plan.

Enabling slow SAI query execution details will produce larger log entries because of the addition information provided.
The log messages for slow queries can take up almost 10x the (uncompressed) disk space. 
In the worst of the worst cases, with all queries being permanently slower than 500ms, 
and at least 50 different queries, reporting every 5 seconds (the default), 
the slow query logger can produce around 120MB of text per day and instance. 
With the detailed logging, it would be 1.14GB per day.

The index query metrics are:
* `sstablesHit`: Number of sstables visited by the query.
* `segmentsHit`: Number of index segments having results for the query.
* `keysFetched`: Number of partition/row keys fetched from the indexes and that will be used to fetch rows from the base table.
  They will be either partition keys in `aa` index format, or row keys in the later row-aware disk formats.
* `partitionsFetched`: Number of live partitions fetched from the storage engine, before post-filtering.
* `partitionsReturned`: Number of live partitions returned to the coordinator, after post-filtering.
  If it is lower than the number of fetched partitions it means that filtering is used.
  Filtering can happen because there are not-indexed restrictions on the query.
  It can also happen because the index query plan decides not to use indexed restrictions.
  Also, the `aa` index format in a table with clustering key will use within-partition filtering.
  Much filtering has a high performance cost and maybe better modeling or indexing can help.
* `partitionTombstonesFetched`: Number of deleted partitions that have been fetched.
  Many tombstones can have a significant performance cost.
* `rowsFetched`: Number of live rows fetched from the storage engine, before post-filtering.
  If there are many rows per partitions it means that there are large partitions.
  Partitions over a few tens of thousands rows are generally problematic for performance,
  and better modelling should be considered.
* `rowsReturned`: Number of live rows returned to the coordinator, after post-filtering.
  If it is lower than the number of fetched rows it means that filtering is used.
  Much filtering has a high performance cost and maybe better modeling or indexing can help.
* `rowTombstonesFetched`: Number of deleted individual rows or ranges of rows that have been fetched.
  Many tombstones can have a significant performance cost.
* `trieSegmentsHit`: Number of trie (literal or primary key) segments visited by the query.
* `triePostingsSkips`: Number of times the query has jumped to the position of a row ID within a trie (literal) posting list.
* `triePostingsDecodes`: Number of times the query has advanced into a trie (literal) posting list.
* `bkdSegmentsHit`: Number of BKD (numeric) segments visited by the query.
* `bkdPostingListsHit`: Number of BKD (numeric) merged posting lists visited by the query.
* `bkdPostingsSkips`: Number of times the query has jumped to the position of a row ID within a BKD (numeric) posting list.
* `bkdPostingsDecodes`: Number of times the query has advanced into a BKD (numeric) posting list.
* `annGraphSearchLatencyNanos`: Time spent searching ANN graph, in nanoseconds.

The index query plan is a tree representation of that plan, where the leaves are actual index reads. 
Each tree node contains estimates about the number of returned rows and the cost of retrieving them.

Here is an example:
```
1 operations were slow in the last 5001 msecs:
<SELECT * FROM k.t WHERE k = ? AND c < ? AND c >= ? AND v1 = ? AND v2 = ? LIMIT 5000 ALLOW FILTERING>, time 1109 msec - slow timeout 500 msec/cross-node
  SAI slow query metrics:
    sstablesHit: 8
    segmentsHit: 8
    keysFetched: 1
    partitionsFetched: 1
    partitionsReturned: 1
    partitionTombstonesFetched: 0
    rowsFetched: 222053
    rowsReturned: 2
    rowTombstonesFetched: 0
    trieSegmentsHit: 2
    triePostingsSkips: 0
    triePostingsDecodes: 2
    bkdPostingListsHit: 6
    bkdSegmentsHit: 6
    bkdPostingsSkips: 0
    bkdPostingsDecodes: 1342
    annGraphSearchLatencyNanos: 0
  SAI slow query plan:
    Limit 5000 (rows: 0.0, cost/row: 15743601.7, cost: 112585.6..112799.5)
     └─ Filter c >= ? AND c < ? AND v1 = ? AND v2 = ? (sel: 0.000006793) (rows: 0.0, cost/row: 15743601.7, cost: 112585.6..112799.5)
         └─ Fetch (rows: 2.0, cost/row: 106.9, cost: 112585.6..112799.5)
             └─ LiteralIndexScan of v2_index (sel: 0.000000004, step: 1.0) (keys: 2.0, cost/key: 0.1, cost: 112585.6..112585.8)
                predicate: Expression{name: v2, op: EQ, lower: (?, true), upper: (?, true), exclusions: []}
```
This example is showing a single-partition query. We know this because the table schema contains `PRIMARY KEY(k, c)`.
The number of fetched and returned partitions is 1, as one would expect from a single-partition query.
There are no tombstones, so that's not likely a problem.

The number of fetched rows, 222053, is much higher than the number of fetched partitions, 1.
This means that the query has found a probably too large partitions, which contributes to performance issues.
If queries are unacceptably slow, we should probably try to remodel to use smaller partitions. 

The number of fetched rows, 222053, is also much higher than the number of returned rows, 2.
This means that a lot of filtering is being done in that partition, and that's likely a cause of the query slowness.
If we look at the query plan, we see that only an index on the restriction on `v2` non-primary key column is used.
There is however another query restriction on `v1` non-primary key column. 
That could be what is causing the query slowness. We should consider adding an index on that column.

More importantly, the numbers of index row keys fetched is 1, whereas the number of fetched rows is 222053.
This means that the index is using the `aa` index format, which only indexes partition keys.
The index uses that partition key to retrieve the large partition and filters almost everything from it.
This combined with the large partition is very inefficient, and it is the most likely cause of the performance issue.
We should either consider using a post-`aa` row-aware index format, 
or try to remodel to reduce the size of the partitions.

On a final note, the query indicates `LIMIT 5000`. Since 5000 is the default fetch size of the Java driver client,
it is possible that the query didn't have a limit and that limit has been added by paging. That's indistinguishable
from the original user query really using a limit equals or greater than 5000. The `ALLOW FILTERING` cause is 
meaningless because queries are always printed with it.
