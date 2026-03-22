# LSIEE Performance Notes

## Target Gates

- Query performance under 100ms for small local CSV/Excel datasets
- File indexing throughput above 20 files/second on the benchmark corpus
- Startup and CLI checks complete quickly in a local `venv`
- Full test suite remains practical for iterative development

## Benchmark Commands

```bash
venv/bin/pytest tests/performance/test_benchmarks.py -q
venv/bin/pytest tests/integration/test_complete_workflow.py -q
```

## Coverage-Backed Performance Checks

- `tests/performance/test_benchmarks.py::test_indexing_benchmark`
  - Verifies indexing 120 files in under 5 seconds
  - Verifies throughput above 20 files/second
- `tests/performance/test_benchmarks.py::test_reindexing_benchmark`
  - Verifies unchanged-file detection stays under 3 seconds
- `tests/integration/test_complete_workflow.py::test_complete_workflow`
  - Verifies the full Phase 1-10 stack works together in one isolated run

## Tuning Notes

- SQLite uses WAL mode and tuned pragmas from `lsiee.storage.schemas`.
- Metadata writes use batched insert/update helpers in `lsiee.storage.metadata_db`.
- Semantic search uses TF-IDF with local JSON vector storage to keep resource usage low on modest systems.

## Latest Baseline

- Benchmark command: `venv/bin/pytest tests/performance/test_benchmarks.py -q`
  - Result: `2` tests passed
- Measured local indexing baseline on the 120-file benchmark corpus:
  - First-pass indexing duration: `0.1498s`
  - First-pass throughput: `801.06 files/sec`
  - Repeat-pass duration: `0.0099s`
  - Repeat-pass unchanged classification: `120 files`
- Full Phase 10 repository verification:
  - `venv/bin/pytest -q` passed with `72` tests and `81%` total coverage
  - `venv/bin/python scripts/verify_installation.py` passed
  - `venv/bin/python -m black --check lsiee tests scripts` passed
  - `venv/bin/python -m isort --check-only lsiee tests scripts` passed
  - `venv/bin/python -m flake8 lsiee tests scripts` passed
