#!/usr/bin/env bash
# EC2: after the data move, IBM 1M and 10M (ASH 2-bit, 380 dims, Cassandra default graph, cndb-like), recall sweep at LIMIT 10.
until grep -q "MOVE DONE\|MISMATCH" ~/move-100m.log; do sleep 30; done
grep -q MISMATCH ~/move-100m.log && { echo "$(date +%H:%M) move failed, queue stopped" >> ~/ash-integration/runs/status; exit 1; }
R=~/ash-integration; B=/mnt/basetmp/base_vectors.fvecs; Q=/data/datasets/ibm-datapile-1b-100m/profiles/base/query_vectors.fvecs
O=enable_ash=true,similarity_function=dot_product,ash_dimensions=380
$R/ctiming-ec2.sh ec2-ibm-1m $B 1000000 $O $Q; cp -r $R/runs /mnt/basetmp/ash-integration-runs
$R/ctiming-ec2.sh ec2-ibm-10m $B 10000000 $O $Q; cp -r $R/runs /mnt/basetmp/ash-integration-runs
echo "$(date +%H:%M) queue-ec2 done" >> $R/runs/status
