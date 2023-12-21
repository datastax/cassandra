# SAI Vector ANN Query Execution

## Storage-Attached Index Basics

* We can create indexes on columns to support searching them without requiring `ALLOW FILTERING` and without requiring
that they are part of the primary key
* An indexed column index consists of local indexes for each memtable and each sstable segment within the table
* Query execution scatters across each index to get the collection of Primary Keys that satisfy a predicate
* Each sstable segment's index is immutable
* Memtable indexes are mutable and are updated as the memtable is updated

## Vector Index Basics

* A vector index gives us the ability to search for similar vectors
* We take advantage of the fact that each sstable segment is immutable and finite
* If we take the top k vectors from each sstable segment, we can materialize them from storage and get the top k vectors
  from the entire table (more on this later)
* The `K` in `topK` is generally the `LIMIT` of the query, but can be larger (more on this later)

## Query Types

### Vector Only Query

When a query is only limited by ANN, the query execution is very simple. The execution follows this path:
1. Query each sstable's vector index(es) to get the top k vectors, which are represented as ordinals.
2. Map ordinals to Primary Keys and sort into ascending Primary Key order to simplify deduplication.
3. Merge/deduplicate the result with a `RangeUnionIterator` while maintaining PK ordering. Produces up to `3 * k` PKs.
4. Materialize each row from storage.
5. Compute the vector similarity score for each row to get the top k rows for the whole table.

```mermaid
---
title: "SELECT * FROM my.table ORDER BY vec ANN OF [...] LIMIT 10"
---
graph LR
    subgraph 1: Get topK
      G[SSTable A\nVector Index]
      H[SSTable B\nVector Index]
      I[Memtable\nVector Index]
    end
    subgraph "2: Map & Sort"
        X[Ordinal -> PK]
        Y[Ordinal -> PK]
        Z[Ordinal -> PK]
    end
    subgraph 3: Merge
        J
    end
    G --top k--> X --> J[Range\nUnion\nIterator]
    H --top k--> Y --> J
    I --top k--> Z --> J
    subgraph "4: Materialize"
        K[Unfiltered\nPartition\nIterator]
    end
    subgraph "5: Compute Score & Filter"
        L[Global top k]
    end
    J --top 3 * k--> K
    K --> L
```

Notes:
* Range queries on the Primary Key that do not require an index are supported and are considered ANN only.
* `ALLOW FILTERING` is not supported.

### Pre-fitered Boolean Predicates Combined with ANN Query

When a query relies on non vector SAI indexes and an ANN ordering predicate, the query execution is more complex. The execution
of query `SELECT * FROM my.table WHERE x = 1 AND y = 'foo' ORDER BY vec ANN OF [...] LIMIT 10` follows this path:
1. Query each boolean predicate's index to get the Primary Keys that satisfy the predicate.
2. Merge the results with a `RangeUnionIterator` that deduplicates results for the predicate and maintains PK ordering.
3. Intersect the results with a `RangeIntersectionIterator` to get the Primary Keys that satisfy all boolean predicates.
4. Materialize the Primary Keys that satisfy all boolean predicates.
5. Map resulting Primary Keys back to row ids and search each vector index for the local top k ordinals, then map those to 
Primary Keys. **This is expensive.**
6. Materialize each row from storage.
7. Compute the vector similarity score for each row.
8. Return the global top k rows.

```mermaid
---
title: "SELECT * FROM my.table WHERE A = 1 AND B = 'foo' ORDER BY vec ANN OF [...] LIMIT 10"
---
graph LR
    subgraph Step 1 and 2: Query Boolean Predicates
        subgraph Indexes on Column A
            A[SSTable 1\nIndex] --A=1--> B[Range\nUnion\nIterator]
            C[SSTable 2\nIndex] --A=1--> B
            D[Memtable\nIndex] --A=1--> B
        end
        subgraph Indexes on Column B
            M[SSTable 1\nIndex] --B='foo'--> N[Range\nUnion\nIterator]
            P[SSTable 2\nIndex] --B='foo'--> N
            O[Memtable\nIndex] --B='foo'--> N
        end
    end
    subgraph Step 3: Find PKs\nMatching Both\nPredicates
      N --> E[Range\nIntersection\nIterator]
      B --> E
    end
    E --> F[Materialize\nALL\nPrimary Keys]
    subgraph "Steps 4 & 5: Index on Column vec"
        F --> G1[PK -> SSTable 1\nRowIds] --> G[SSTable 1\nVector Index] --> X[Ordinal -> PK]
        F --> H1[PK -> SSTable 2\nRowIds] --> H[SSTable 2\nVector Index] --> Y[Ordinal -> PK]
        F --> I[Memtable\nVector Index]
        X --top k--> J[Range\nUnion\nIterator]
        Y --top k--> J
        I --top k---> J
    end
    subgraph "Step 6: Materialize"
      K[Unfiltered\nPartition\nIterator]
    end
    subgraph "Step 7: Compute Score & Filter"
      L[Global top k]
    end
    J --top 3 * k--> K[Unfiltered\nPartition\nIterator]
    K --top k--> L[Global top k]
```
