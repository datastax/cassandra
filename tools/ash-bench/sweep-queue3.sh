#!/usr/bin/env bash
# Third B queue (after queue2): PQ at 1M with Cassandra's default PQ, for ASH-vs-PQ recall and QPS at equal depth.
D=/mnt/raid0/mnemo-20261114/cass-timing; R=$D/rerank-sweep
until grep -q "queue2 done" $R/status; do sleep 30; done
C=/mnt/raid0/datasets-clean; IBM=/mnt/raid0/datasets/ibm-datapile-1b-1m/profiles/base
$D/sweep-b.sh pq-ada-982790 $C/ada_002_1M_base_982790.fvecs $C/ada_002_1M_query_10000.fvecs 982790 similarity_function=dot_product
$D/sweep-b.sh pq-ibm-1000000 $IBM/base_vectors.fvecs $IBM/query_vectors.fvecs 1000000 similarity_function=dot_product
$D/sweep-b.sh pq-mpnet-999812 $C/mpnet-1m/wiki_mpnet_embeddings_base_999812.fvecs $C/mpnet-1m/wiki_mpnet_embeddings_query_10000.fvecs 999812 similarity_function=dot_product
$D/sweep-b.sh pq-cohere-1000000 $C/cohere-1m/cohere_wiki_en_flat_base_1m_norm.fvecs $C/cohere-1m/cohere_wiki_en_flat_query_10k_norm.fvecs 1000000 similarity_function=dot_product
echo "$(date +%H:%M) queue3 done" >> $R/status
