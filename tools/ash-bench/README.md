# ASH benchmark tooling (experiment only)

Scripts used to measure SAI vector compaction time, recall and rerank depth with
ASH and PQ on benchmark servers. Nothing here is meant for a production PR.

- `test/unit/.../sai/cql/CompactionTimingTest.java`: the driver. It streams an
  fvecs file into a table, builds the index per SSTable, compacts, and reports
  times. With `-Dbench.sweepLimits`, it computes exact ground truth over the
  loaded vectors and sweeps recall@LIMIT against `rerank_k`.
- `CompactionGraph.java` on this branch adds phase timers (CGTIMING lines).
- `sweep-*.sh`, `ctiming-ec2.sh`, `*queue*.sh`: run wrappers. Paths are those of
  the benchmark servers; adjust them for another machine.
- `cassandra-cndb-like*.yaml`: test config with cndb-like SAI settings (segment
  buffer, one large compacted segment).
- `knee2.py`, `fit.py`: find the smallest rerank depth within 1.0 recall point
  of the plateau for each LIMIT, and fit k = a + b * LIMIT.

The forked test JVM heap is set with `-Dtest.maxmemory` (default 1024m).
