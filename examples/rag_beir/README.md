# LanceForge × BEIR SciFact: End-to-End RAG Example

Real-world validation: ingest the BEIR SciFact corpus, build an ANN index, run all test queries, measure **nDCG@10** and **Recall@100** against ground truth. Compare distributed LanceForge vs single-machine lancedb baseline.

## Why this test matters

Phase 17 Week 1 benchmark used synthetic Gaussian vectors → recall@10 = 1.000 everywhere (trivial). Real embedding distributions are different. This example is the first time the system meets real data.

## Dataset

**BEIR SciFact**:
- 5,183 documents (scientific claims abstracts)
- 300 queries (claim verification)
- Ground truth: binary relevance qrels

Download: <https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip>

## Pipeline

```
scifact/corpus.jsonl ──┐
                        ├──► embed (sentence-transformers) ──► vectors
scifact/queries.jsonl ──┘
                                                                │
                          ┌──────────────────────────────────── ▼
                          │                LanceForge CreateTable + CreateIndex
                          │                                    │
                          │                    run 300 queries ▼
                          │                          scores + retrieved doc IDs
                          │                                    │
                          ▼                                    ▼
              Single-machine lancedb baseline            compute nDCG@10, R@100
                 (same embedding, same queries)           vs scifact/qrels.tsv
```

## Run

```bash
pip install beir sentence-transformers

# Start LanceForge cluster (see QUICKSTART.md)

python3 examples/rag_beir/run_beir_scifact.py
```

Outputs:
- `results.json` — numerical metrics
- `RESULTS.md` — human-readable comparison

## Expected findings

(Run once to populate; this README was written before the first execution.)

- **Recall@100 should be close** between distributed and single-machine (within 0.01-0.02). If not, there's a correctness bug.
- **nDCG@10 should be identical** — ordering quality should not differ.
- **Latency**: distributed will be slightly slower on this tiny (5K docs) dataset (scatter-gather overhead dominates, matches Phase 17 benchmark finding).
