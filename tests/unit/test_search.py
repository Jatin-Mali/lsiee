"""Tests for semantic search."""

from datetime import datetime

from lsiee.file_intelligence.indexing.embedding_indexer import EmbeddingIndexer
from lsiee.file_intelligence.search.embeddings import EmbeddingModel
from lsiee.file_intelligence.search.semantic_search import SemanticSearch
from lsiee.file_intelligence.search.text_extractor import TextExtractor
from lsiee.storage.metadata_db import FileRecord, MetadataDB
from lsiee.storage.schemas import initialize_database
from lsiee.storage.vector_db import VectorDB


def test_embedding_generation():
    """Test embedding generation."""
    model = EmbeddingModel()

    texts = ["Hello world", "Python programming"]
    embeddings = model.encode(texts)

    assert len(embeddings) == 2
    assert len(embeddings[0]) > 0
    assert len(embeddings[0]) == len(embeddings[1])


def test_text_extractor_chunking():
    """Test text chunking."""
    extractor = TextExtractor()

    chunks = extractor.chunk_text("abcdefghij", chunk_size=4)

    assert chunks == ["abcd", "efgh", "ij"]


def test_vector_db_search_returns_ranked_results(tmp_path):
    """Test that VectorDB ranks the most relevant document first."""
    vector_db = VectorDB(tmp_path / "vectors")
    vector_db.add_embeddings(
        ids=["/tmp/python.txt", "/tmp/cooking.txt"],
        embeddings=[[], []],
        documents=["Python code bug fix function", "Fresh herbs and cooking recipes"],
        metadatas=[
            {"filename": "python.txt", "extension": ".txt"},
            {"filename": "cooking.txt", "extension": ".txt"},
        ],
    )

    results = vector_db.search("bug fix in python code", n_results=2)

    assert results["ids"][0][0] == "/tmp/python.txt"
    assert len(results["ids"][0]) == 2


def test_semantic_search_empty_store(tmp_path):
    """Test semantic search on an empty store."""
    search = SemanticSearch(vector_db_path=tmp_path / "vectors")

    results = search.search("test query", max_results=5)

    assert results == []


def test_embedding_indexer_indexes_pending_files(tmp_path):
    """Test pending files are indexed into the semantic store."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    initialize_database(db_path)

    sample_file = tmp_path / "notes.txt"
    sample_file.write_text("Python bug fix and function notes", encoding="utf-8")

    with MetadataDB(db_path) as db:
        db.insert_file(
            FileRecord(
                id=None,
                path=str(sample_file),
                filename=sample_file.name,
                extension="txt",
                size_bytes=sample_file.stat().st_size,
                modified_at=datetime.fromtimestamp(sample_file.stat().st_mtime),
            )
        )

    indexer = EmbeddingIndexer(db_path=db_path, vector_db_path=vector_db_path)
    indexed_count = indexer.index_all_pending()

    assert indexed_count == 1
    assert VectorDB(vector_db_path).count() == 1


def test_semantic_search_returns_relevant_results(tmp_path):
    """Test semantic search returns the most relevant file first."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    initialize_database(db_path)

    bug_file = tmp_path / "bugfix.py"
    bug_file.write_text("def fix_bug():\n    return 'bug fix in python code'\n", encoding="utf-8")
    notes_file = tmp_path / "notes.txt"
    notes_file.write_text("gardening checklist and watering tips", encoding="utf-8")

    with MetadataDB(db_path) as db:
        for file_path in (bug_file, notes_file):
            db.insert_file(
                FileRecord(
                    id=None,
                    path=str(file_path),
                    filename=file_path.name,
                    extension=file_path.suffix.lstrip("."),
                    size_bytes=file_path.stat().st_size,
                    modified_at=datetime.fromtimestamp(file_path.stat().st_mtime),
                )
            )

    indexer = EmbeddingIndexer(db_path=db_path, vector_db_path=vector_db_path)
    assert indexer.index_all_pending() == 2

    search = SemanticSearch(db_path=db_path, vector_db_path=vector_db_path)
    results = search.search("python bug fix", max_results=5)

    assert results
    assert results[0]["metadata"]["filename"] == "bugfix.py"
    assert results[0]["final_score"] >= results[-1]["final_score"]
