#!/usr/bin/env bash
# A: one 100k segment per dataset (Cassandra default graph, ASH 2-bit at dim/2; IBM at 380 of 384 dims).
D=$HOME/ash-integration/rerank-sweep; C=$HOME/datasets-clean
A="enable_ash=true,similarity_function=dot_product"
run() { $D/sweep-a.sh $1 $C/$2_base_$3.fvecs $C/$2_query_10000.fvecs 100000 "$A${4:-}"; }
run ada100k ada_002_100k 99287
run cohere-en-v3-100k cohere_english_v3_100k 99685
run e5-small-100k e5_small_v2_100k 99443
run e5-base-100k e5_base_v2_100k 99446
run e5-large-100k e5_large_v2_100k 99460
run gecko-100k gecko_100k 98391
run oai-large1536-100k openai_v3_large_1536_100k 98716
run oai-large3072-100k openai_v3_large_3072_100k 98714
run oai-small1536-100k openai_v3_small_1536_100k 98548
$D/sweep-a.sh ibm100k $C/ibm-datapile/base_vectors.fvecs $C/ibm-datapile/query_vectors.fvecs 100000 "$A,ash_dimensions=380"
echo "$(date +%H:%M) queue-a done" >> $D/status
