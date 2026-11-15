#!/usr/bin/env bash
# First queue: ada002 and IBM at 25k, 50k, 100k (fit), Cassandra default graph, ASH 2-bit.
D=/mnt/raid0/mnemo-20261114/cass-timing; R=$D/rerank-sweep; mkdir -p $R
ADA=/mnt/raid0/datasets-clean; IBM=/mnt/raid0/datasets/ibm-datapile-1b-1m/profiles/base
for n in 100000 25000 50000; do
  $D/sweep-b.sh ada-$n $ADA/ada_002_1M_base_982790.fvecs $ADA/ada_002_1M_query_10000.fvecs $n enable_ash=true,similarity_function=dot_product
  $D/sweep-b.sh ibm-$n $IBM/base_vectors.fvecs $IBM/query_vectors.fvecs $n enable_ash=true,similarity_function=dot_product,ash_dimensions=380
done
echo "$(date +%H:%M) queue1 done" >> $R/status
