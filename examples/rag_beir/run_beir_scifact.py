#!/usr/bin/env python3
"""
BEIR SciFact RAG pipeline on LanceForge.

Ingests the SciFact corpus, embeds with sentence-transformers, builds an
ANN index on a running LanceForge cluster, runs the 300 test queries,
and compares nDCG@10 / Recall@100 against a single-machine lancedb
baseline with identical embeddings.

Prereqs: sentence-transformers, beir, pip install.
Cluster: expect coordinator at 127.0.0.1:9200 (run QUICKSTART.md scripts first).
"""
import os, sys, json, time, struct, zipfile, urllib.request
from pathlib import Path
import numpy as np

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("FATAL: install sentence-transformers (pip install sentence-transformers)")
    sys.exit(1)

try:
    from beir.datasets.data_loader import GenericDataLoader
    from beir.retrieval.evaluation import EvaluateRetrieval
except ImportError:
    print("FATAL: install beir (pip install beir)")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parents[2] / 'lance-integration/tools'))
import pyarrow as pa
import pyarrow.ipc as ipc
import grpc
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

COORD_ADDR = os.environ.get('LANCEFORGE_COORD', '127.0.0.1:9200')
DATASET_DIR = Path(__file__).parent / 'datasets'
DATASET_DIR.mkdir(exist_ok=True)
RESULTS_DIR = Path(__file__).parent / 'results'
RESULTS_DIR.mkdir(exist_ok=True)

MODEL_NAME = 'sentence-transformers/all-MiniLM-L6-v2'   # 384-dim, small & fast
DIM = 384
K = 100


def download_scifact():
    zip_path = DATASET_DIR / 'scifact.zip'
    extract_dir = DATASET_DIR / 'scifact'
    if extract_dir.exists():
        return extract_dir
    url = 'https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip'
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(DATASET_DIR)
    return extract_dir


def load():
    path = download_scifact()
    corpus, queries, qrels = GenericDataLoader(data_folder=str(path)).load(split="test")
    print(f"Loaded {len(corpus)} docs, {len(queries)} queries, qrels for {len(qrels)} queries")
    return corpus, queries, qrels


def embed(model, texts, batch_size=64):
    return model.encode(texts, batch_size=batch_size, show_progress_bar=True,
                        convert_to_numpy=True, normalize_embeddings=True)


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema); w.write_batch(batch); w.close()
    return sink.getvalue().to_pybytes()


def ev(v): return struct.pack(f"<{len(v)}f", *v.tolist())


def make_stub():
    return pbg.LanceSchedulerServiceStub(
        grpc.insecure_channel(COORD_ADDR,
            options=[("grpc.max_receive_message_length", 512 * 1024 * 1024),
                     ("grpc.max_send_message_length", 512 * 1024 * 1024)]))


# ──────────────────────────────────────────────────────────────────────────
# Distributed path (LanceForge)
# ──────────────────────────────────────────────────────────────────────────

def run_distributed(doc_ids, doc_vecs, query_ids, query_vecs):
    stub = make_stub()
    # Table with doc_id as int64 (beir uses string doc ids; we map)
    int_to_str = {i: d for i, d in enumerate(doc_ids)}
    # Drop any stale table
    try: stub.DropTable(pb.DropTableRequest(table_name="scifact"))
    except grpc.RpcError: pass

    # CreateTable (chunks if needed)
    print("[dist] CreateTable with SciFact corpus")
    CHUNK = 2000  # ~3 MB per chunk
    first = min(CHUNK, len(doc_ids))
    batch = pa.record_batch([
        pa.array(range(first), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(doc_vecs[:first].flatten()), list_size=DIM),
    ], names=['id', 'vector'])
    r = stub.CreateTable(pb.CreateTableRequest(
        table_name="scifact", arrow_ipc_data=ipc_bytes(batch)))
    if r.error: raise RuntimeError(r.error)
    print(f"[dist] CreateTable: {r.num_rows} rows")

    off = first
    while off < len(doc_ids):
        end = min(off + CHUNK, len(doc_ids))
        br = pa.record_batch([
            pa.array(range(off, end), type=pa.int64()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(doc_vecs[off:end].flatten()), list_size=DIM),
        ], names=['id', 'vector'])
        ar = stub.AddRows(pb.AddRowsRequest(
            table_name="scifact", arrow_ipc_data=ipc_bytes(br)))
        if ar.error: raise RuntimeError(ar.error)
        off = end
    print(f"[dist] Total rows: {off}")

    # Search
    print(f"[dist] Running {len(query_ids)} queries")
    t0 = time.perf_counter()
    retrieved = {}
    for qid, qv in zip(query_ids, query_vecs):
        r = stub.AnnSearch(pb.AnnSearchRequest(
            table_name="scifact", vector_column="vector",
            query_vector=ev(qv), dimension=DIM, k=K, nprobes=20,
            metric_type=2))  # 2=Dot (normalized → cosine)
        if r.error: raise RuntimeError(r.error)
        t = ipc.open_stream(r.arrow_ipc_data).read_all() if r.arrow_ipc_data else None
        hits = {}
        if t is not None and t.num_rows > 0:
            # Use retrieval rank as score: higher = better. Lance returns
            # results already sorted by the distance metric, so position is
            # the cleanest cross-metric signal. (Raw `_distance` semantics
            # differ per metric type and can confuse BEIR's evaluator.)
            ids = t.column("id").to_pylist()
            for rank, idx in enumerate(ids):
                hits[int_to_str[idx]] = float(len(ids) - rank)  # K..1
        retrieved[qid] = hits
    wall = time.perf_counter() - t0
    print(f"[dist] {len(query_ids)} queries in {wall:.1f}s ({len(query_ids)/wall:.1f} QPS)")
    return retrieved, wall


# ──────────────────────────────────────────────────────────────────────────
# Baseline path (single-machine lancedb)
# ──────────────────────────────────────────────────────────────────────────

def run_baseline(doc_ids, doc_vecs, query_ids, query_vecs):
    """Pure-numpy cosine similarity baseline (no lancedb dependency).
    Acts as ground truth for the distributed path's ordering quality."""
    int_to_str = {i: d for i, d in enumerate(doc_ids)}
    # Pre-normalize (embeddings are already normalized, but be explicit)
    docs_n = doc_vecs / np.linalg.norm(doc_vecs, axis=1, keepdims=True).clip(min=1e-9)
    queries_n = query_vecs / np.linalg.norm(query_vecs, axis=1, keepdims=True).clip(min=1e-9)

    print(f"[baseline] Exhaustive cosine: {len(query_ids)} queries × {len(doc_ids)} docs")
    t0 = time.perf_counter()
    # Batch dot product: (Q, dim) × (dim, D) → (Q, D)
    sims = queries_n @ docs_n.T
    # Top-K indices per query
    top_k = np.argpartition(-sims, K, axis=1)[:, :K]
    retrieved = {}
    for qi, qid in enumerate(query_ids):
        row_sims = sims[qi, top_k[qi]]
        order = np.argsort(-row_sims)
        hits = {}
        for rank, pos in enumerate(order):
            doc_idx = int(top_k[qi, pos])
            hits[int_to_str[doc_idx]] = float(K - rank)
        retrieved[qid] = hits
    wall = time.perf_counter() - t0
    print(f"[baseline] {len(query_ids)} queries in {wall:.1f}s ({len(query_ids)/wall:.1f} QPS)")
    return retrieved, wall


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main():
    corpus, queries, qrels = load()

    # Flatten
    doc_ids = list(corpus.keys())
    doc_texts = [corpus[d]['title'] + ' ' + corpus[d].get('text', '')
                 for d in doc_ids]
    query_ids = list(queries.keys())
    query_texts = [queries[q] for q in query_ids]

    print(f"Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    print(f"Embedding {len(doc_texts)} documents")
    doc_vecs = embed(model, doc_texts).astype(np.float32)

    print(f"Embedding {len(query_texts)} queries")
    query_vecs = embed(model, query_texts).astype(np.float32)

    # Try distributed first, fall back gracefully
    results = {}
    try:
        retrieved_dist, dist_wall = run_distributed(doc_ids, doc_vecs, query_ids, query_vecs)
        ndcg, _map, recall, precision = EvaluateRetrieval.evaluate(qrels, retrieved_dist, [10, 100])
        results['distributed'] = {
            'nDCG@10': ndcg['NDCG@10'],
            'Recall@100': recall['Recall@100'],
            'wall_s': dist_wall,
            'qps': len(query_ids) / dist_wall,
        }
        print(f"[dist] nDCG@10={ndcg['NDCG@10']:.4f}  Recall@100={recall['Recall@100']:.4f}")
    except Exception as e:
        print(f"Distributed path FAILED: {e}")
        results['distributed'] = {'error': str(e)}

    try:
        retrieved_bl, bl_wall = run_baseline(doc_ids, doc_vecs, query_ids, query_vecs)
        ndcg_b, _map_b, recall_b, prec_b = EvaluateRetrieval.evaluate(qrels, retrieved_bl, [10, 100])
        results['baseline'] = {
            'nDCG@10': ndcg_b['NDCG@10'],
            'Recall@100': recall_b['Recall@100'],
            'wall_s': bl_wall,
            'qps': len(query_ids) / bl_wall,
        }
        print(f"[baseline] nDCG@10={ndcg_b['NDCG@10']:.4f}  Recall@100={recall_b['Recall@100']:.4f}")
    except Exception as e:
        print(f"Baseline path FAILED: {e}")
        results['baseline'] = {'error': str(e)}

    # Persist
    out_json = RESULTS_DIR / 'results.json'
    with open(out_json, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Wrote {out_json}")

    # Human report
    out_md = RESULTS_DIR / 'RESULTS.md'
    with open(out_md, 'w') as f:
        f.write("# BEIR SciFact Results\n\n")
        f.write(f"Model: {MODEL_NAME}  Docs: {len(doc_ids)}  Queries: {len(query_ids)}\n\n")
        f.write("| Metric | Distributed | Baseline | Δ |\n")
        f.write("|---|---|---|---|\n")
        d = results.get('distributed', {}); b = results.get('baseline', {})
        for m in ['nDCG@10', 'Recall@100', 'qps', 'wall_s']:
            dv = d.get(m, 'N/A'); bv = b.get(m, 'N/A')
            delta = ''
            if isinstance(dv, (int, float)) and isinstance(bv, (int, float)) and bv != 0:
                delta = f"{(dv - bv) / bv * 100:+.1f}%"
            dv_s = f"{dv:.4f}" if isinstance(dv, float) else str(dv)
            bv_s = f"{bv:.4f}" if isinstance(bv, float) else str(bv)
            f.write(f"| {m} | {dv_s} | {bv_s} | {delta} |\n")
    print(f"Wrote {out_md}")


if __name__ == '__main__':
    main()
