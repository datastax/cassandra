#!/usr/bin/env bash
# Experiment tooling, not for production: Cassandra timing runs on EC2: ctiming-ec2.sh <label> <base> <n> <options> [queries]
# Input is read from EBS; Cassandra data lives on /data (instance storage, wiped on stop: copy results off).
L=$1; BASE=$2; N=$3; OPTS=$4; Q=${5:-}
D=$HOME/ash-integration/runs; mkdir -p $D; rm -rf /data/cass-test; mkdir -p /data/cass-test
cd ~/ash-integration/cassandra
export JAVA_HOME=$HOME/.jdks/jdk-22.0.2+9 PATH=$HOME/.jdks/jdk-22.0.2+9/bin:$HOME/tools/apache-ant-1.10.15/bin:$PATH
OUT=$D/$L.txt; rm -f $OUT
EXTRA="-Dbench.vectors=$BASE -Dbench.n=$N -Dbench.sstables=${BENCH_SSTABLES:-4} -Dbench.out=$OUT -Djvector.vectorization_provider=native -Djvector.ash.queryProjection=int8 -Dcassandra.config=file://$HOME/ash-integration/cassandra-cndb-like-ec2.yaml -Dunified_compaction.override_ucs_config_for_vector_tables=true"
[ -n "$Q" ] && EXTRA="$EXTRA -Dbench.queries=$Q -Dbench.nq=1000 -Dbench.sweepLimits=${LIMITS:-10}"
ant testsome -Dtest.maxmemory=80g -Dtest.name=org.apache.cassandra.index.sai.cql.CompactionTimingTest -Dtest.timeout=86400000 -Dcassandra.test.processorCount=16 "-Dtest.jvm.args=$EXTRA -Dbench.options=$OPTS" > $D/$L.log 2>&1
echo "$(date +%H:%M) $L rc=$? $(grep -o "Tests run.*" $D/$L.log | head -1)" >> $D/status
