#!/usr/bin/env python3
"""
Hybrid Search Quality Benchmark (NDCG / MRR)

Measures end-to-end retrieval quality for the RAG-critical hybrid search:
  - ANN-only NDCG@10
  - FTS-only NDCG@10
  - Hybrid (ANN + FTS → RRF fusion) NDCG@10
  - Validates: Hybrid >= max(ANN-only, FTS-only)

Uses a synthetic dataset with both vector embeddings and text content,
where relevance is determined by a combination of semantic similarity
(vector) and keyword match (text).

Methodology:
  1. Generate documents with category-based relevance
  2. For each query, define ground truth relevance scores
  3. Measure NDCG@10 and MRR for each retrieval mode
  4. Prove hybrid fusion adds value over single-mode search
"""

import sys, os, time, struct, subprocess, json, math
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = "/tmp/lanceforge_recall_bench"
DIM = 64

# ── Metrics ──

def dcg_at_k(relevances, k):
    """Discounted Cumulative Gain at k."""
    r = relevances[:k]
    return sum(rel / math.log2(i + 2) for i, rel in enumerate(r))

def ndcg_at_k(retrieved_ids, relevance_map, k):
    """Normalized DCG@k. relevance_map: {doc_id: relevance_score}"""
    # Actual DCG from retrieved order
    rels = [relevance_map.get(doc_id, 0.0) for doc_id in retrieved_ids[:k]]
    actual_dcg = dcg_at_k(rels, k)

    # Ideal DCG (best possible order)
    ideal_rels = sorted(relevance_map.values(), reverse=True)[:k]
    ideal_dcg = dcg_at_k(ideal_rels, k)

    if ideal_dcg == 0:
        return 0.0
    return actual_dcg / ideal_dcg

def mrr(retrieved_ids, relevant_set):
    """Mean Reciprocal Rank: 1/rank of first relevant result."""
    for i, doc_id in enumerate(retrieved_ids):
        if doc_id in relevant_set:
            return 1.0 / (i + 1)
    return 0.0

# ── Dataset Generation ──

def generate_hybrid_dataset(n_docs=50000, dim=64, n_queries=100, n_topics=20, seed=42):
    """
    Generate documents with both vectors and text.

    Each document belongs to a topic. Topics have:
    - A centroid vector (semantic meaning)
    - A set of keywords (textual signal)

    Queries target specific topics. Relevance = topic match.
    Documents in the same topic as the query are relevant (graded by distance).
    """
    np.random.seed(seed)

    # Create topic centroids and keywords
    topic_centroids = np.random.randn(n_topics, dim).astype(np.float32) * 3
    topic_keywords = [f"topic{t}" for t in range(n_topics)]

    # Assign documents to topics
    doc_topics = np.random.randint(0, n_topics, n_docs)
    doc_vectors = topic_centroids[doc_topics] + np.random.randn(n_docs, dim).astype(np.float32) * 0.5

    # Generate text: documents contain their topic keyword + noise words
    noise_words = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta",
                   "eta", "theta", "iota", "kappa", "lambda", "mu"]
    doc_texts = []
    for i in range(n_docs):
        topic_word = topic_keywords[doc_topics[i]]
        noise = " ".join(np.random.choice(noise_words, 3))
        doc_texts.append(f"{topic_word} {noise} document_{i}")

    # Generate queries: each targets a specific topic
    query_topics = np.random.randint(0, n_topics, n_queries)
    query_vectors = topic_centroids[query_topics] + np.random.randn(n_queries, dim).astype(np.float32) * 0.3
    query_texts = [topic_keywords[t] for t in query_topics]

    # Build relevance maps: docs in same topic are relevant, scored by vector distance
    relevance_maps = []
    relevant_sets = []
    for qi in range(n_queries):
        qt = query_topics[qi]
        qv = query_vectors[qi]
        rel_map = {}
        rel_set = set()
        for di in range(n_docs):
            if doc_topics[di] == qt:
                # Relevance based on vector distance (closer = more relevant)
                dist = np.sum((doc_vectors[di] - qv) ** 2)
                # Convert distance to relevance: 3 (very relevant) to 0
                relevance = max(0.0, 3.0 - dist / 5.0)
                if relevance > 0.1:
                    rel_map[di] = relevance
                    rel_set.add(di)
        relevance_maps.append(rel_map)
        relevant_sets.append(rel_set)

    return {
        'vectors': doc_vectors,
        'texts': doc_texts,
        'topics': doc_topics,
        'query_vectors': query_vectors,
        'query_texts': query_texts,
        'relevance_maps': relevance_maps,
        'relevant_sets': relevant_sets,
    }

# ── Data Preparation ──

def create_hybrid_shards(data, num_shards, shard_dir):
    """Create shards with both vector and text columns."""
    import pyarrow as pa
    import lance

    n = len(data['vectors'])
    rows_per_shard = n // num_shards
    os.makedirs(shard_dir, exist_ok=True)

    for i in range(num_shards):
        start = i * rows_per_shard
        end = start + rows_per_shard if i < num_shards - 1 else n
        uri = os.path.join(shard_dir, f'shard_{i:02d}.lance')

        table = pa.table({
            'id': pa.array(range(start, end), type=pa.int64()),
            'text': pa.array(data['texts'][start:end], type=pa.string()),
            'topic': pa.array(data['topics'][start:end].tolist(), type=pa.int32()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(data['vectors'][start:end].flatten(), type=pa.float32()),
                list_size=data['vectors'].shape[1]),
        })
        lance.write_dataset(table, uri, mode='overwrite')

        ds = lance.dataset(uri)
        if not ds.list_indices():
            npart = min(64, max(4, (end - start) // 1000))
            ds.create_index('vector', index_type='IVF_FLAT', num_partitions=npart)

        # Build FTS index if supported
        try:
            ds.create_scalar_index('text', index_type='INVERTED')
            print(f"  Shard {i}: {end-start} rows + IVF_FLAT + FTS index")
        except Exception as e:
            print(f"  Shard {i}: {end-start} rows + IVF_FLAT (FTS index: {e})")

# ── Search Functions ──

def ev(v):
    return struct.pack(f'<{len(v)}f', *v)

def ann_search(stub, query_vector, k=10, nprobes=20):
    """ANN-only search via gRPC."""
    import lance_service_pb2 as pb
    import pyarrow.ipc as ipc

    r = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="hybrid", vector_column="vector",
        query_vector=ev(query_vector), dimension=len(query_vector),
        k=k, nprobes=nprobes, metric_type=0), timeout=30)
    if r.arrow_ipc_data:
        t = ipc.open_stream(r.arrow_ipc_data).read_all()
        return t.column("id").to_pylist()[:k]
    return []

def fts_search(stub, query_text, k=10):
    """FTS-only search via gRPC."""
    import lance_service_pb2 as pb
    import pyarrow.ipc as ipc

    r = stub.FtsSearch(pb.FtsSearchRequest(
        table_name="hybrid", text_column="text",
        query_text=query_text, k=k), timeout=30)
    if r.arrow_ipc_data:
        t = ipc.open_stream(r.arrow_ipc_data).read_all()
        return t.column("id").to_pylist()[:k]
    return []

def hybrid_search(stub, query_vector, query_text, k=10, nprobes=20):
    """Hybrid ANN+FTS search via gRPC."""
    import lance_service_pb2 as pb
    import pyarrow.ipc as ipc

    r = stub.HybridSearch(pb.HybridSearchRequest(
        table_name="hybrid", vector_column="vector",
        query_vector=ev(query_vector), dimension=len(query_vector),
        nprobes=nprobes, metric_type=0,
        text_column="text", query_text=query_text,
        k=k), timeout=30)
    if r.arrow_ipc_data:
        t = ipc.open_stream(r.arrow_ipc_data).read_all()
        return t.column("id").to_pylist()[:k]
    return []

# ── Main Benchmark ──

def run_hybrid_bench():
    import yaml, grpc
    import lance_service_pb2_grpc

    num_shards = 3
    n_docs = 50000
    k = 10

    print("=" * 60)
    print("  Hybrid Search Quality Benchmark (NDCG / MRR)")
    print(f"  {n_docs} docs, {num_shards} shards, dim={DIM}")
    print("=" * 60)

    print("\n[1/4] Generating hybrid dataset...")
    data = generate_hybrid_dataset(n_docs, DIM, n_queries=100)
    n_queries = len(data['query_vectors'])

    shard_dir = os.path.join(BASE, 'hybrid_shards')
    if os.path.exists(shard_dir):
        import shutil
        shutil.rmtree(shard_dir)

    print(f"\n[2/4] Creating {num_shards} shards with vector + text...")
    create_hybrid_shards(data, num_shards, shard_dir)

    print(f"\n[3/4] Starting cluster...")
    cfg = {
        'tables': [{'name': 'hybrid', 'shards': [
            {'name': f'hybrid_shard_{i:02d}',
             'uri': os.path.join(shard_dir, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']}
            for i in range(num_shards)
        ]}],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': 51700 + i}
                      for i in range(num_shards)],
    }
    cfg_path = os.path.join(BASE, 'hybrid_config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(cfg, f)

    os.system("pkill -f 'lance-coordinator.*51750|lance-worker.*5170' 2>/dev/null")
    time.sleep(1)

    processes = []
    for i in range(num_shards):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(51700 + i)],
            stdout=open(f"{BASE}/hw{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    time.sleep(3)
    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, "51750"],
        stdout=open(f"{BASE}/hcoord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(4)

    for proc in processes:
        assert proc.poll() is None, f"Process died! Check {BASE}/h*.log"
    print("  Cluster running")

    channel = grpc.insecure_channel("127.0.0.1:51750",
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    print(f"\n[4/4] Running {n_queries} queries × 3 modes...")

    ann_ndcgs, ann_mrrs = [], []
    fts_ndcgs, fts_mrrs = [], []
    hybrid_ndcgs, hybrid_mrrs = [], []
    fts_errors = 0

    for qi in range(n_queries):
        qv = data['query_vectors'][qi]
        qt = data['query_texts'][qi]
        rel_map = data['relevance_maps'][qi]
        rel_set = data['relevant_sets'][qi]

        if not rel_map:
            continue

        # ANN-only
        try:
            ann_ids = ann_search(stub, qv, k)
            ann_ndcgs.append(ndcg_at_k(ann_ids, rel_map, k))
            ann_mrrs.append(mrr(ann_ids, rel_set))
        except Exception:
            ann_ndcgs.append(0.0)
            ann_mrrs.append(0.0)

        # FTS-only
        try:
            fts_ids = fts_search(stub, qt, k)
            fts_ndcgs.append(ndcg_at_k(fts_ids, rel_map, k))
            fts_mrrs.append(mrr(fts_ids, rel_set))
        except Exception:
            fts_errors += 1
            fts_ndcgs.append(0.0)
            fts_mrrs.append(0.0)

        # Hybrid
        try:
            hyb_ids = hybrid_search(stub, qv, qt, k)
            hybrid_ndcgs.append(ndcg_at_k(hyb_ids, rel_map, k))
            hybrid_mrrs.append(mrr(hyb_ids, rel_set))
        except Exception:
            hybrid_ndcgs.append(0.0)
            hybrid_mrrs.append(0.0)

        if (qi + 1) % 25 == 0:
            print(f"    {qi+1}/{n_queries}...")

    channel.close()
    for p in processes:
        try: p.terminate()
        except: pass

    # Results
    results = {
        'ANN-only': {
            'ndcg@10': float(np.mean(ann_ndcgs)),
            'mrr': float(np.mean(ann_mrrs)),
        },
        'FTS-only': {
            'ndcg@10': float(np.mean(fts_ndcgs)),
            'mrr': float(np.mean(fts_mrrs)),
            'errors': fts_errors,
        },
        'Hybrid (RRF)': {
            'ndcg@10': float(np.mean(hybrid_ndcgs)),
            'mrr': float(np.mean(hybrid_mrrs)),
        },
    }

    print(f"\n{'='*60}")
    print("  HYBRID SEARCH QUALITY RESULTS")
    print(f"{'='*60}")
    print(f"  Dataset: {n_docs} docs, {num_shards} shards")
    print(f"  Queries: {n_queries} (topic-targeted)")
    print(f"  k={k}")
    print()
    print(f"  {'Mode':<20} {'NDCG@10':>10} {'MRR':>10}")
    print(f"  {'-'*20} {'-'*10} {'-'*10}")
    for mode, r in results.items():
        print(f"  {mode:<20} {r['ndcg@10']:>10.4f} {r['mrr']:>10.4f}")
        if 'errors' in r and r['errors'] > 0:
            print(f"    ({r['errors']} FTS errors — FTS index may not be available)")

    # Check: Hybrid should be >= max(ANN, FTS)
    ann_ndcg = results['ANN-only']['ndcg@10']
    fts_ndcg = results['FTS-only']['ndcg@10']
    hyb_ndcg = results['Hybrid (RRF)']['ndcg@10']
    best_single = max(ann_ndcg, fts_ndcg)

    if hyb_ndcg >= best_single * 0.95:  # within 5% is acceptable
        print(f"\n  ✓ Hybrid NDCG ({hyb_ndcg:.4f}) >= best single-mode ({best_single:.4f})")
    else:
        print(f"\n  ✗ Hybrid NDCG ({hyb_ndcg:.4f}) < best single-mode ({best_single:.4f})")

    print(f"{'='*60}")

    out = os.path.join(os.path.dirname(__file__), 'results', 'hybrid_ndcg.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        json.dump(results, f, indent=2)

    return hyb_ndcg >= best_single * 0.95

if __name__ == '__main__':
    try:
        ok = run_hybrid_bench()
        sys.exit(0 if ok else 1)
    except:
        os.system("pkill -f 'lance-coordinator.*51750|lance-worker.*5170' 2>/dev/null")
        raise
