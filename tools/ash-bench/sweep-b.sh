#!/usr/bin/env bash
# Experiment tooling, not for production: rerank-depth sweep on B: sweep-b.sh <label> <base> <queries> <n> <options>
L=$1; BASE=$2; Q=$3; N=$4; OPTS=$5
D=/mnt/raid0/mnemo-20261114/cass-timing/rerank-sweep; mkdir -p $D
cd ~/ash-integration/cassandra
export JAVA_HOME=$HOME/.jdks/jdk-22.0.2+9 PATH=$HOME/.jdks/jdk-22.0.2+9/bin:$HOME/tools/apache-ant-1.10.15/bin:$PATH
OUT=$D/$L.txt; rm -f $OUT
Y=/mnt/raid0/mnemo-20261114/cass-timing/cassandra-cndb-like.yaml
EXTRA="-Dbench.vectors=$BASE -Dbench.queries=$Q -Dbench.n=$N -Dbench.nq=1000 -Dbench.out=$OUT -Dbench.sweepLimits=${LIMITS:-1,5,10,20,50,100} -Djvector.vectorization_provider=native -Djvector.ash.queryProjection=int8 -Dcassandra.config=file://$Y -Dunified_compaction.override_ucs_config_for_vector_tables=true"
ant testsome -Dtest.maxmemory=48g -Dtest.name=org.apache.cassandra.index.sai.cql.CompactionTimingTest -Dtest.timeout=10800000 -Dcassandra.test.processorCount=32 "-Dtest.jvm.args=$EXTRA -Dbench.options=$OPTS" > $D/$L.log 2>&1
echo "$(date +%H:%M) $L rc=$? $(grep -o "Tests run.*" $D/$L.log | head -1)" >> $D/status
