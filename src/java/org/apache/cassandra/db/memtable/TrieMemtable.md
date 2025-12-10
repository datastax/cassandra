# TrieMemtable

This file describes the implementation of `TrieMemtable` and the various trie-backed containers that we use
to convert the trie representation to legacy structures.

Trie memtables store the structure of the memtable in one single trie. If viewed as a simple map, this trie
maps a cell key to its data, where the cell key is composed by concatenating the byte-comparable
representations of all key components:
- token
- partition key
- clustering key
- column id
- cell path

To maintain the correct order, we use the 
[byte-comparable representation](../../utils/bytecomparable/ByteComparable.md) 
of the keys everywhere in tries
and thus will omit "byte-comparable representation" in the text below (in other words, when we say e.g.
"indexed by the cell path" below we mean "indexed by the byte comparable representation of the cell path").

Because tries naturally perform prefix compression, the leading components of these keys are not repeated
and the storage and processing is at least as efficient as having a hierarchy of containers, but crucially
the trie machinery that operates on these does not need to understand the different types of keys or use
separate containers. Additionally, because of this prefix compressed structure we can easily find the points
of origin of various levels of the hierarchy, and can thus view their branches (i.e. everything that has the
same prefix, e.g. same decorated partition key, with that prefix removed; we call this a "tail trie" for that
prefix) as an implementation of the legacy container they map to.

The trie memtable maintains separate deletion paths which originate at the partition level and contain the
hierarchy of deletions and the specific deletion time applicable to any point in the trie. See
[the deletion-aware tries section in Trie.md](../tries/Trie.md#deletion-aware-tries) for information about
how deletion branches work.

The details of these mappings will be given below.

## Structure

### Cell

The cell is the lowest level of the data hierarchy (stored at the leaves of the trie) and contains:
- value
- timestamp
- ttl / local deletion time

If a cell is part of a complex column, it also needs the cell path by which it is reached inside the
complex column (e.g. the map key). When a cell is deleted (which can happen both because it was explicitly
deleted or because its ttl expired), its value is removed and the cell becomes a tombstone.

As of the current implementation, `TrieMemtable` uses the legacy `Cell` object directly, with one difference:
if a cell is part of a complex column, we don't store the cell path inside the object. If a cell is deleted 
(which is a rare occurence for cells in a memtable), we still use a `Cell` and store it on the live part of
the trie[^1].

[^1]: The main reason for this is the fact that cells can become deleted without any change other than time
elapsed, and we will always have the possibility of deleted cells being present in the live branches.
Because expiration should be very rare for data in memtables, we don't expect the accumulation of this
kind of tombstones to become a problem. This point is to be revisited when we implement on-disk tries and
compaction.

### Complex column

Complex columns are collections of cells with cell paths. We map this to tries where the `Cell` objects
without cell path are stored in a trie map with the cell paths as keys. When we need to return a cell
from these containers to a legacy consumer, we combine the `Cell` object with the path used to reach
it. To make it easier to list the columns contained in a row, we mark the root of a complex column with
a special `COMPLEX_COLUMN_MARKER`, a singleton object that contains no data.

Complex columns can be deleted as a whole (i.e. have the so-called "complex deletion"). We store complex
deletions as a deleted branch at the root of the complex column. Note that a complex column can be
in the live path, in both, but also in the deletion path only, if it has been deleted and no newer value
has been added.

The class `TrieBackedComplexColumn` implements the mapping between a trie branch and the legacy complex
column concept. These complex columns cannot be constructed on their own and are always taken from larger
trie objects (e.g. a row or a memtable) to represent the column when a legacy consumer needs this form.

#### Example

The example below shows the trie that describes a complex column of type `map<uuid, double>`, which is
created with the following insert statement:
```
INSERT INTO %s (..., purchases) values (..., {"d79012af-8b34-4fb4-9799-6c0d29ca4e2f" : 88.67,
                                              "830b82ce-a7f2-4939-9ea1-46b9d3714848" : 168.01})
```
```
-> COMPLEX_COLUMN_MARKER
*** Start deletion branch
-> LIVE -> deletedAt=345[COLUMN]
↑ -> deletedAt=345[COLUMN] -> LIVE
*** End deletion branch
404830b82cea7f29399ea146b9d371484838 -> [purchases[?]=168.01 ts=346]
  4d79012af8b34fb497996c0d29ca4e2f38 -> [purchases[?]=88.67 ts=346]
```
When we insert a value (rather than update) in a complex column, Cassandra always creates a tombstone with
a smaller timestamp. Here we see this as a deletion branch which starts a deletion before the root of the
trie and ends it after the root, covering any data that may have previously existed for the complex column.

On the live side of the trie, we have a `COMPLEX_COLUMN_MARKER` at the root and two trie branches for the
two entries in the map. The UUID keys are converted to byte-ordered by moving the UUID type digit first
and are only present in the path in the trie. When we convert these to cells this path would be converted
back to a UUID.

### Row

The row is the central concept in Cassandra's CQL data model. A row is a collection of typed columns. The
type and order of columns is predefined in the table's metadata, and thus each column can be identified by
a simple integer id[^2], which we represent as a variable length unsigned integer, usually taking just one
byte. Some of the columns are simple, mapping to a single cell, and some may be complex,
where the id maps to the complex column marker, and the individual cells can be reached by following the
cell path from the position of the marker. A row may also contain "liveness info", which tells Cassandra
if the row should be listed as live even if all cells that it contains have been deleted. We store this
liveness info as a `LivenessInfo` object at the root of a row and use it as a marker to list rows within
a partition.

[^2]: Provided that the metadata does not change, which we can be guaranteed for the lifetime of a memtable.

Rows can have a row deletion, which is represented as a branch deletion over the root of the row in the
deletion branch of the trie. Like complex columns, it is also possible for rows to exist in the deletion
branch alone, and to be able to recognize such rows as rows we mark the root of a row in a deletion branch
with a row level marker, which is a special `TrieTombstoneMarker` that has no effect other than as metadata
to mark a level.

There are two ways that the content of a row can be listed:
- by cell, in which case we present all the cells/leaves of the trie, combining complex column cells with
  their cell path.
- by column, in which case we present simple cells directly, and also look for `COMPLEX_COLUMN_MARKER` or
  a deletion marker below the root of the row. If one of these is found, we take the tail trie (which
  includes both the live and deleted part) and use it to form a `TrieBackedComplexColumn`.

Rows are implemented by the class `TrieBackedRow`. They can be taken from a bigger structure or constructed
by inserting cells into a standalone row in a short-lived in-memory trie using a row builder. We use these
standalone tries as the building blocks to make the partition update objects that we insert into a memtable.

#### Example

The full row for the example above, where the insert statement also sets the `total` column:
```
INSERT INTO %s (..., total, purchases) values (..., 256.68, {"d79012af-8b34-4fb4-9799-6c0d29ca4e2f" : 88.67,
                                                             "830b82ce-a7f2-4939-9ea1-46b9d3714848" : 168.01})
```
is the following:
```
-> [ts=346]
*** Start deletion branch
-> Level ROW
01 -> LIVE -> deletedAt=345[COLUMN]
01↑ -> deletedAt=345[COLUMN] -> LIVE
↑ -> Level ROW
*** End deletion branch
00 -> [total=256.68 ts=346]
01 -> COMPLEX_COLUMN_MARKER
  404830b82cea7f29399ea146b9d371484838 -> [purchases[?]=168.01 ts=346]
    4d79012af8b34fb497996c0d29ca4e2f38 -> [purchases[?]=88.67 ts=346]
```

This trie contains row level markers in both the live (liveness info `[ts=346]`) and deletion
(`Level ROW`)[^3] branches; a complex column deletion for the column `purchases` with index `01`;
a cell for the timestamp and value of the simple column `total` with index `00`, and two cells with path
for the entries of the `purchases` complex column.

[^3]: Note that the deletion marker must be presented both before and after the branch because
it needs to be returned before the content of the branch in both forward and reverse direction.

When this trie is presented as an iterator of `ColumnData`, `TrieBackedRow` uses `tailTries` to stop on
cells, deletions and complex column markers and view the trie above as:
```
00 -> [total=256.68 ts=346]
01 -> [complex column]
```
where the stop at `01` is given the complex column trie as shown in the example in the previous section as
the tail trie.

### Partition

A partition is an ordered collection of rows, where each row is indexed by a "clustering key", formed of
one or more columns of a pre-specified type. We represent these as tries which can be seen as:
- a collection of cells, indexed by the clustering key, column id and optionally a cell path, with some
  metadata added at the trie nodes that start each row and complex column; or
- a collection of row tries, indexed by the clustering key, stored together in a single trie object.

As before, the root of a partition is marked by an instance of the `PartitionMarker` interface. This marker
interface has no methods; in standalone partitions we use the singleton `PARTITION_MARKER`, but in partitions
that are part of a memtable we use objects that are also used to collect statistics.

If the partition has a static row, we do not treat it differently, i.e. we use the `STATIC_CLUSTERING` it
reports as the key for the static row subtrie inside the partition.

Partitions can be deleted as a whole, which we do by creating a branch deletion for the partition root.
Importantly, ranges of rows inside a partition can also be deleted using a range tombstone. We implement
the latter as range deletions in the deletion branch of the partition; the byte-comparable mapping of
range tombstone bounds and boundaries is chosen in a way that makes sure such deletion ranges cover the
trie sections containing any of the deleted rows.

The deletion information for a partition is stored separately in the trie in a "deletion branch", which
is presented at the root of the partition. This separation is needed for several reasons:
- To be able to find the applicable deletion (the most recent of the applicable partition, range, row or
  complex column tombstone) for any point in the trie by finding the closest deletion in the deletion
  branch. This closest deletion may be millions on live entries away, and if the two were stored together
  we would have to walk over all these live entries to find it.
- To be able to find the closest live entry to a given position easily, when there may be millions of
  tombstones between that position and the live entry. By separating the deletion branch we can simply
  advance in the live part of the trie.

Note that when we take a branch of the trie representing a smaller container, e.g. a row, we follow both
the live and deletion branch and present any data we have together in the tail trie.

Listing the content of a partition is usually accomplished by taking a so-called "unfiltered" row iterator
between a set if pairs of clustring bounds, containing all the rows and deletions applicable to that set.
We perform this by taking the intersection of the partition trie with the set between the bounds[^4],
and then walking the combination of the live and deletion trie to find: 
- row markers (i.e. `LivenessInfo` objects or deletion boundaries with a row marker); when we find one we
  take the tail trie and wrap it into a `TrieBackedRow`, or
- deletion boundaries, which we map into the corresponding `RangeTombstoneBound(ary)`.

[^4]: Inclusivity does not matter for this because clustering bounds do not match row clustering keys; they
include a component that adjusts them to be just before or just after a row key.

Note that if a deletion range applies over a wider range than the query, the result of this intersection
needs to restrict the deletion to the queried range. The trie code ensures that this is done.

The partition representation is implemented in `TrieBackedPartition`. It can be created over the tail trie in
a memtable, or standalone in a short-lived in-memory trie. The most common usage of standalone partitions is
`TriePartitionUpdate`, which is built by adding rows into an initially empty trie. When Cassandra executes a 
write request, it first turns it into one or more `TriePartitionUpdate` objects, and then merges these into
the current memtable.

#### Example

The example below is constructed by the following statements:
```
INSERT INTO %s (..., date, total, purchases) 
       VALUES (..., '2026-02-12', 324.83, {"82b4ce57-d6a0-4470-8747-1c2aa4fc5961": 324.83})
DELETE FROM %s WHERE ... AND date = '2026-02-09'
DELETE FROM %s WHERE ... AND date <= '2026-01-31' AND date >= '2026-01-01'
```

```
-> partition with 1 rows and 8 tombstones
*** Start deletion branch
4080004fe620 -> LIVE -> deletedAt=412[RANGE]
      500460 -> deletedAt=412[RANGE] -> LIVE
        0d38 -> Level ROW + LIVE -> deletedAt=329[ROW]
          38↑ -> Level ROW + deletedAt=329[ROW] -> LIVE
        1038 -> Level ROW
            01 -> LIVE -> deletedAt=366[COLUMN]
            01↑ -> deletedAt=366[COLUMN] -> LIVE
          38↑ -> Level ROW
*** End deletion branch
408000501038 -> [ts=367]
            00 -> [total=324.83 ts=367]
            01 -> COMPLEX_COLUMN_MARKER
              40482b4ce57d6a047087471c2aa4fc596138 -> [purchases[?]=324.83 ts=367]
```

The trie here has a partition marker with some collected statistics ("partition with ...") 
and is first indexed by the row clustering key (the `date` column), which is encoded as an integer
and wrapped in a clustering sequence container (seen as the `40` leading byte and `38`/`20`/`60` terminators).

Here we have one live row at `408000501038`, including a deletion for its complex column, and two types of
row deletions: a deleted row at `408000500d38` and a range tombstone between `4080004fe620` and `408000500460`.
Notice how as we move to the bigger partition container the deletion branches are moved to be split at the
higher point, repeating the clustering key path &mdash; in the memtable trie all deletion branches split at
the partition level to be efficiently processed (see 
[section in Trie.md](../tries/Trie.md#why-predetermined-deletion-levels-deletionsatfixedpoints-are-important)
for the reasons for this choice).

Live rows are recognized by their `LivenessInfo` marker `[ts=367]`. Deleted rows (partial or not) have a level
marker shown above as `Level ROW`. Full row deletions are applied as a branch deletion covering the row
(boundary `LIVE -> deleted` at `408000500d38` and `deleted -> LIVE` at `408000500d38↑` (i.e. on the return path, 
after the children of that point); these boundaries combine with the level marker and are reported together.
Range tombstones use the clustering bound terminators `20` (before row) and `60` (after row) and are
expressed as range boundaries to span the trie sections between the two ends[^5].

[^5]: It is possible to express row deletions using these clustering bound terminators as well, having
the effect of converting the deletion from a `key = X` restriction to `key >= X AND key <= X`. Cassandra
could just as well work with the latter only; we tried this approach for a while and gave it up for two
reasons: the existing test suite needs to make a distinction between the two types of deletion; and having
the start and end at the same point in the trie presents some optimization opportunities.

A `TrieBackedPartition` is usually consumed by converting it to an `UnfilteredRowIterator`. This is
achieved by calling `tailTries` and recognizing the row and range tombstone markers as described in the
previous paragraph, viewing the partition as:
```
4080004fe620 -> LIVE -> deletedAt=412 (range tombstone start)
      500460 -> deletedAt=412 -> LIVE (range tombstone end)
        0d38 -> [row recognized by the marker "Level ROW" with tail]
        1038 -> [row recognized by the marker "ts=367" with tail]
```
where each row is turned into a `TrieBackedRow` by passing in the tail trie originating at that point, and the
path that leads to the point to be converted to a clustering key object.

For example, the tail given for the `408000501038` row is
```
-> [ts=367]
*** Start deletion branch
-> Level ROW
01 -> LIVE -> deletedAt=366[COLUMN]
01↑ -> deletedAt=366[COLUMN] -> LIVE
↑ -> Level ROW
*** End deletion branch
00 -> [total=324.83 ts=367]
01 -> COMPLEX_COLUMN_MARKER
  40482b4ce57d6a047087471c2aa4fc596138 -> [purchases[?]=324.83 ts=367]
```

### Memtable

A memtable is a giant trie containing a map of all the cells indexed by the concatenation of all cell key
components. In other words, it is a map of partition tries, indexed by the partition's decorated partition
key (i.e. token + serialized partition key), stored together in one giant structure.

This partition map does not have any deletion component, because in Cassandra we cannot have deletions that
go above the level of individual partitions. Deletion branches are always rooted at the partition level.

The memtable is split into memtable shards, i.e. separate tries that split the served token range in equal
ranges, each holding a long-lived in-memory trie. This is done to improve parallelism, because individual
in-memory tries cannot be modified by multiple threads concurrently. By splitting the space into shards we
allow per-shard parallelism, which typically improves the parallelism of the whole structure by the number
of shards because the randomized nature of the tokens usually results in well-distributed accesses to the
individual shards.

While writes are handled by the individual shards, the memtable also contains a merged view of the shards
which is used to serve reads &mdash; given a key or bounds to query the merge can automatically handle the
question of finding the relevant shard that contains the data. Since tries can be read concurrently by
multiple threads including while they are being modified, we do not need to lock the shard to perform a read.

#### Example
The trie below
```
40a9e72bd32b9f1ba24041434d450038 -> partition with 2 rows and 4 tombstones
                                *** Start deletion branch
                                408000500e38 -> Level ROW
                                            01 -> LIVE -> deletedAt=345[COLUMN]
                                            01↑ -> deletedAt=345[COLUMN] -> LIVE
                                          38↑ -> Level ROW
                                *** End deletion branch
                                408000500e38 -> [ts=346]
                                            00 -> [total=256.68 ts=346]
                                            01 -> COMPLEX_COLUMN_MARKER
                                              404830b82cea7f29399ea146b9d371484838 -> [purchases[?]=168.01 ts=346]
                                                4d79012af8b34fb497996c0d29ca4e2f38 -> [purchases[?]=88.67 ts=346]
                                        1138 -> [ts=385]
                                            00 -> [total=99.23 ts=385]
                                            01 -> COMPLEX_COLUMN_MARKER
                                              404dab4819dc6f5c05b5754a057d78c99a38 -> [purchases[?]=99.23 ts=385]
  ca8e7ee71a25ce664049424d0038 -> partition with 1 rows and 4 tombstones
                              *** Start deletion branch
                              408000500f38 -> Level ROW
                                          01 -> LIVE -> deletedAt=351[COLUMN]
                                          01↑ -> deletedAt=351[COLUMN] -> LIVE
                                        38↑ -> Level ROW
                              *** End deletion branch
                              408000500f38 -> [ts=352]
                                          00 -> [total=542.79 ts=352]
                                          01 -> COMPLEX_COLUMN_MARKER
                                            40435441ee93ac90d098e89c75b414addb38 -> [purchases[?]=420.66999999999996 ts=352]
                                                edf143aa8d178f86b4d83a72a7517038 -> [purchases[?]=122.12 ts=352]
  cd0a37fd8f053c6c404170706c650038 -> partition with 1 rows and 8 tombstones
                                  *** Start deletion branch
                                  4080004fe620 -> LIVE -> deletedAt=412[RANGE]
                                        500460 -> deletedAt=412[RANGE] -> LIVE
                                          0d38 -> Level ROW + LIVE -> deletedAt=329[ROW]
                                            38↑ -> Level ROW + deletedAt=329[ROW] -> LIVE
                                          1038 -> Level ROW
                                              01 -> LIVE -> deletedAt=366[COLUMN]
                                              01↑ -> deletedAt=366[COLUMN] -> LIVE
                                            38↑ -> Level ROW
                                  *** End deletion branch
                                  408000501038 -> [ts=367]
                                              00 -> [total=324.83 ts=367]
                                              01 -> COMPLEX_COLUMN_MARKER
                                                40482b4ce57d6a047087471c2aa4fc596138 -> [purchases[?]=324.83 ts=367]
```
is constructed by running the code in `TrieMemtableDocTrieMakerTest.java` and represents the full trie for a small
table with three partitions. The leading part of this trie is the decorated partition key, composed of a token
and serialization of the partition key (the `company` column, containing a string), wrapped in a sequence encoding
(see [description in ByteComparable.md](../../utils/bytecomparable/ByteComparable.md#multi-component-sequences-partition-or-clustering-keys-tuples-bounds-and-nulls)).
The first byte `40` starts the sequence encoding, followed by 8 bytes of Murmur3-generated token, another `40` byte
to start the next value in the sequence, a `00`-terminated string for the company name, and a `38` sequence
terminator.

At each partition key we have a partition marker that also collects statistics about the partition, and each
partition is as described in the previous section, with deletion branch splitting at the partition root.

Then the memtable is consumed (e.g. on flush or to list a range of partitions), we once again use `tailTries`
recognizing these partition markers to view it as:
```
40a9e72bd32b9f1ba24041434d450038 -> [partition with tail]
  ca8e7ee71a25ce664049424d0038 -> [partition with tail]
  cd0a37fd8f053c6c404170706c650038 -> [partition with tail]
```
and we use the tail trie and the path used to reach the point to form a `TrieBackedPartition`. E.g. the example
trie in the previous section is given as the tail for the key `40cd0a37fd8f053c6c404170706c650038`, which is
translated to the partition key "Apple".

## Other key points

### Write atomicity and monotonicity

Cassandra provides some consistency guarantees for writes to the same partition on the same node, namely that
such writes are atomic (i.e. a reader cannot see part of an update and miss something else that was modified
with the same update) and, if requests are made to the same node, that writes to the same partition are
monotonic (i.e. if one process issues two writes one after the other, no reader can see the result of the
latter write without the result of the former).

To ensure these two properties, the memtable performs trie mutations with a force-copy predicate that
recognizes partition boundaries. The effect of this predicate is that whenever a mutation reaches the partition
level, every change to a node at that level or below it in the trie is performed by making a copy of the
trie cells rather than modifying them in place. 

This has the effect of practically maintaining a snapshot of the partition's state before the mutation and
letting any concurrent reader that has reached a node inside that snapshot observe it unchanged. Eventually
the mutation will reach a point above the partition level and modify a pointer to the new version of the
partition, which will swap in the new version of the whole partition for later readers to see. From this point
on the old snapshot can no longer be found, and eventually all readers that could be inside it will finish
their work and the old snapshot can be thrown away.

See also [the Atomicity and Consistency sections in InMemoryTrie.md](../tries/InMemoryTrie.md#atomicity).

### Preventing corruption from reuse

As the memtable trie is long-lived, it should be able to identify cells of the trie that have been replaced
with newer versions and reuse them. For this to work, it needs to be able to tell if all operations started
before a given point in time have been completed. If e.g. it issues such a "read op barrier" after a write
wires in a new version of a partition, and that barrier "expires" (i.e. guarantees that all operations
started before the barrier's issue), then it can be certain that none of the currently active or future
operations on the memtable can see the previous snapshot of that partition and all its nodes can thus be
recycled and reused.

This doesn't need to be granular to a partition or individual cells; we can form batches of cells to reuse
and issue a single barrier for the whole group. To do this, we use the `readOrdering` `OpOrder` that 
`ColumnFamilyStore` already provides. Every read on a table marks itself in this op order and closes it
when it completes, and it gives us the required signal for reusing space in the memtable tries.

There is an additional situation where memory containing memtable data could be reused if we are not careful;
this is a feature shared by all memtables implementations: if data is stored off-heap, it can happen
that some results of a read reference this data long after the whole memtable is deleted and has released
its memory. This can occur, for example, if this node is the coordinator for a request with multiple
replicas and it has to wait for responses from other nodes. As it is not a good idea to block reuse for the
long periods that such responses can require, we need a different way to ensure that such data is alive.

The latter problem is solved by copying data to on-heap objects for temporary storage before it is given
to the coordination layer to keep. This has a pretty high overhead and will likely be replaced with trie
serialization in later stages of the work on trie-based interfaces.

### Unfiltered and filtered row iterators

When serving a query, unfiltered row iterators from multiple sources are merged to form a single stream and
then "filtered", i.e. processed to remove all deletions and report the final result. To aid the filtering
process, a trie-backed partition's iterator can be asked to `stopIssuingTombstones`, which tells it to stop
looking at the deletion branch and makes it possible to quickly return live data without having to skip over
tombstones that may be between it and the current position of the iterator.

This currently only works for partitions that are present only in a memtable, because the deletions need
to be applied to data from other sources. This should be improved with later work on on-disk tries and query
interfaces.

### Applying deletions and handling dangling markers

When the memtable receives a mutation that contains deletions, the trie code applies these deletions to all
content that they apply to / cover. This means that anything with a lower timestamp than the deletion time
is removed from the memtable, including cells, liveness info or earlier tombstones. When such a deletion is
applied, the substructure that leads to the deleted cells is also removed, which also means that bigger
branches like rows can become empty and should be removed as well.

There is a little complication here caused by level markers. If we don't do anything special about them, a
deletion, for example, that deletes a partition would remove all cells, the rows' liveness info, all range
or row tombstones, but would keep an empty liveness info object as a marker for the root of each row that
existed before. This marker would have no substructure and represent an empty row, but unfortunately this 
marker would also force the path leading to it to be retained, inflating the size of the memtable and 
complicating later walks that have to pass over it.

To avoid this problem and make sure that we delete markers that no longer serve any purpose, the in-memory
trie mutation code accepts a `danglingMetadataCleaner` argument that is used to check if a trie content/metadata
entry makes sense when it has no substructure. The checker is called every time the mutation code notices
that it is building a leaf node, and drops the content (and with it the whole leaf and path leading to it)
if the checker returns `true`. By passing a suitable cleaner, recognizing `LivenessInfo.EMPTY` and
`COMPLEX_COLUMN_MARKER` in data branches and `Level ROW` in deletion ones, the trie memtable makes sure that
unproductive markers are dropped without affecting things like cells or non-empty liveness info that are 
meaningful without substructure.

Note that when data in the trie and paths are deleted, the trie will drop and reuse the trie cells that stored
them, but cannot do anything about non-trie memory. This means, for example, that cell objects stored in on- or
off-heap slab buffers cannot be released. This fact is a feature of Cassandra memtables that is not changed by
the current trie memtable implementation.

#### Example

Suppose we issue a partition deletion for the 'Apple' parition using
```
DELETE FROM ... USING TIMESTAMP 513 WHERE company = 'Apple';
```

This deletion is represented by the partition update
```
-> PARTITION_MARKER
*** Start deletion branch
-> LIVE -> deletedAt=513[PARTITION]
↑ -> deletedAt=513[PARTITION] -> LIVE
*** End deletion branch
```

To merge it into the trie, we attach it at the path corresponding to its decorated partition key: 
```
40cd0a37fd8f053c6c404170706c650038 -> PARTITION_MARKER
                                  *** Start deletion branch
                                  -> LIVE -> deletedAt=513[PARTITION]
                                  ↑ -> deletedAt=513[PARTITION] -> LIVE
                                  *** End deletion branch
```
and then we call the trie code to merge it in.

The trie code walk this trie in parallel with the in-memory memtable trie. When it reaches the "Apple"
partition it will see something similar to:
```
40cd0a37fd8f053c6c404170706c650038 -> partition with 1 rows and 8 tombstones
                                  *** Start deletion branch
                                  -> TO APPLY: LIVE -> deletedAt=513[PARTITION]
                                  4080004fe620 -> LIVE -> deletedAt=412[RANGE]
                                        500460 -> deletedAt=412[RANGE] -> LIVE
                                          0d38 -> Level ROW + LIVE -> deletedAt=329[ROW]
                                            38↑ -> Level ROW + deletedAt=329[ROW] -> LIVE
                                          1038 -> Level ROW
                                              01 -> LIVE -> deletedAt=366[COLUMN]
                                              01↑ -> deletedAt=366[COLUMN] -> LIVE
                                            38↑ -> Level ROW
                                  ↑ -> TO APPLY: deletedAt=513[PARTITION] -> LIVE
                                  *** End deletion branch
                                  -> TO APPLY: LIVE -> deletedAt=513[PARTITION]
                                  408000501038 -> [ts=367]
                                              00 -> [total=324.83 ts=367]
                                              01 -> COMPLEX_COLUMN_MARKER
                                                40482b4ce57d6a047087471c2aa4fc596138 -> [purchases[?]=324.83 ts=367]
                                  ↑ -> TO APPLY: deletedAt=513[PARTITION] -> LIVE
```

Everything between the "TO APPLY" bounds that has a timestamp smaller than 513 is removed from the trie,
resulting in:
```
40cd0a37fd8f053c6c404170706c650038 -> partition with 1 rows and 2 tombstones
                                  *** Start deletion branch
                                  -> LIVE -> deletedAt=513[PARTITION]
                                  408000500d38 -> Level ROW
                                            38↑ -> Level ROW
                                          1038 -> Level ROW
                                            38↑ -> Level ROW
                                  ↑ -> deletedAt=513[PARTITION] -> LIVE
                                  *** End deletion branch
                                  408000501038 -> [ts=EMPTY]
                                              01 -> COMPLEX_COLUMN_MARKER
```
As part of the process of modifying the in-memory trie, the mutation code recognizes that the `COMPLEX_COLUMN_MARKER`
and the `Level ROW` markers have no children and call the dangling metadata cleaner. As that returns true, the marker
and the path leading to them is removed.
```
40cd0a37fd8f053c6c404170706c650038 -> partition with 1 rows and 2 tombstones
                                  *** Start deletion branch
                                  -> LIVE -> deletedAt=513[PARTITION]
                                  ↑ -> deletedAt=513[PARTITION] -> LIVE
                                  *** End deletion branch
                                  408000501038 -> [ts=EMPTY]
```
Now `[ts=EMPTY]` (i.e. `LivenessInfo.EMPTY`) has no children and has the dangling metadata cleaner called, which
tells the trie code to remove it and the path leading to it, resulting in the final
```
40cd0a37fd8f053c6c404170706c650038 -> partition with 0 rows and 2 tombstones
                                  *** Start deletion branch
                                  -> LIVE -> deletedAt=513[PARTITION]
                                  ↑ -> deletedAt=513[PARTITION] -> LIVE
                                  *** End deletion branch
```
The intermediate stages shown above are not actually materialized and just given for clarification: the real
process yields the final state directly by walking the trie in parallel with the deletions and recursively
setting pointers to null as they are deleted or become empty.