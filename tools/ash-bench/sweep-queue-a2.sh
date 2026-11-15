#!/usr/bin/env bash
# A, second queue: the same 100k datasets with full-dimension 2-bit ASH (second quantization setting), then Cassandra's default PQ.
D=$HOME/ash-integration/rerank-sweep; C=$HOME/datasets-clean
run() { $D/sweep-a.sh $1 $C/$2_base_$3.fvecs $C/$2_query_10000.fvecs $3 "$4"; }
for mode in ashfull pq; do
  for spec in "ada100k ada_002_100k 99287 1536" "cohere-en-v3-100k cohere_english_v3_100k 99685 1024" "e5-small-100k e5_small_v2_100k 99443 384" \
              "e5-base-100k e5_base_v2_100k 99446 768" "e5-large-100k e5_large_v2_100k 99460 1024" "gecko-100k gecko_100k 98391 768" \
              "oai-large1536-100k openai_v3_large_1536_100k 98716 1536" "oai-large3072-100k openai_v3_large_3072_100k 98714 3072" \
              "oai-small1536-100k openai_v3_small_1536_100k 98548 1536"; do
    set -- $spec
    if [ $mode = ashfull ]; then o="enable_ash=true,similarity_function=dot_product,ash_dimensions=$4"; else o="similarity_function=dot_product"; fi
    run $mode-$1 $2 $3 $o
  done
done
echo "$(date +%H:%M) queue-a2 done" >> $D/status
