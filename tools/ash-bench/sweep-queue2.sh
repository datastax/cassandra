#!/usr/bin/env bash
# Second B queue (after queue1): N-series with the wider depth grid, then the 1M check.
D=/mnt/raid0/mnemo-20261114/cass-timing; R=$D/rerank-sweep
until grep -q "queue1 done" $R/status; do sleep 30; done
C=/mnt/raid0/datasets-clean; IBM=/mnt/raid0/datasets/ibm-datapile-1b-1m/profiles/base
A="enable_ash=true,similarity_function=dot_product"
ada() { $D/sweep-b.sh ada-$1 $C/ada_002_1M_base_982790.fvecs $C/ada_002_1M_query_10000.fvecs $1 $A; }
ibm() { $D/sweep-b.sh ibm-$1 $IBM/base_vectors.fvecs $IBM/query_vectors.fvecs $1 $A,ash_dimensions=380; }
mpnet() { $D/sweep-b.sh mpnet-$1 $C/mpnet-1m/wiki_mpnet_embeddings_base_999812.fvecs $C/mpnet-1m/wiki_mpnet_embeddings_query_10000.fvecs $1 $A; }
cohere() { $D/sweep-b.sh cohere-$1 $C/cohere-1m/cohere_wiki_en_flat_base_1m_norm.fvecs $C/cohere-1m/cohere_wiki_en_flat_query_10k_norm.fvecs $1 $A; }
ada 100000; ibm 100000; ada 250000; ibm 250000
mpnet 25000; mpnet 100000; mpnet 250000; cohere 25000; cohere 100000; cohere 250000
ada 982790; ibm 1000000; mpnet 999812; cohere 1000000
echo "$(date +%H:%M) queue2 done" >> $R/status
