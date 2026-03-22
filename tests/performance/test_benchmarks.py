"""Lightweight performance benchmarks for Phase 7."""

from __future__ import annotations

import time

from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.storage.schemas import initialize_database


def _create_corpus(directory, count: int):
    for index in range(count):
        (directory / f"doc_{index:03d}.txt").write_text(
            f"document {index} performance benchmark content\n" * 3,
            encoding="utf-8",
        )


def test_indexing_benchmark(tmp_path):
    """Indexing should sustain a reasonable throughput on small local corpora."""
    db_path = tmp_path / "lsiee.db"
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    initialize_database(db_path).disconnect()
    _create_corpus(corpus_dir, count=120)

    start = time.perf_counter()
    stats = Indexer(db_path=db_path).index_directory(corpus_dir, show_progress=False)
    duration = time.perf_counter() - start
    files_per_second = stats["files_indexed"] / max(duration, 1e-6)

    assert stats["files_indexed"] == 120
    assert duration < 5.0
    assert files_per_second > 20.0


def test_reindexing_benchmark(tmp_path):
    """A repeat pass should be fast and classify files as unchanged."""
    db_path = tmp_path / "lsiee.db"
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    initialize_database(db_path).disconnect()
    _create_corpus(corpus_dir, count=120)

    indexer = Indexer(db_path=db_path)
    indexer.index_directory(corpus_dir, show_progress=False)

    start = time.perf_counter()
    stats = indexer.index_directory(corpus_dir, show_progress=False)
    duration = time.perf_counter() - start

    assert stats["files_indexed"] == 0
    assert stats["files_updated"] == 0
    assert stats["files_unchanged"] == 120
    assert duration < 3.0
