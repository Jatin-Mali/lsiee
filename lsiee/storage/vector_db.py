"""Persistent document store for TF-IDF search."""

import json
import logging
import math
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

from lsiee.config import config, get_vector_db_path
from lsiee.file_intelligence.search.embeddings import EmbeddingModel
from lsiee.security import atomic_write_text, ensure_safe_directory, read_secure_text

logger = logging.getLogger(__name__)


class VectorDB:
    """JSON-backed storage for searchable file documents."""

    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            db_path = get_vector_db_path()

        self.db_path = db_path
        ensure_safe_directory(self.db_path.parent, must_exist=False)
        self.db_path.mkdir(parents=True, exist_ok=True)
        self.vectors_file = self.db_path / "vectors.json"

        self.ids: List[str] = []
        self.embeddings: List[List[float]] = []
        self.documents: List[str] = []
        self.metadatas: List[Dict[str, Any]] = []
        self._load()

    def add_embeddings(
        self,
        ids: List[str],
        embeddings: List[List[float]],
        documents: List[str],
        metadatas: List[Dict[str, Any]],
    ):
        """Upsert indexed file documents and metadata."""
        for id_, emb, doc, meta in zip(ids, embeddings, documents, metadatas):
            normalized_document = self._normalize_document(doc)
            if len(normalized_document) < int(config.get("security.min_search_document_chars", 10)):
                continue
            if id_ in self.ids:
                idx = self.ids.index(id_)
                self.ids.pop(idx)
                self.embeddings.pop(idx)
                self.documents.pop(idx)
                self.metadatas.pop(idx)

            self.ids.append(id_)
            self.embeddings.append(emb)
            self.documents.append(normalized_document)
            self.metadatas.append(meta)

        self._save()
        logger.info("Added %s search documents to vector DB", len(ids))

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        n = min(len(vec1), len(vec2))
        dot = sum(vec1[i] * vec2[i] for i in range(n))
        norm1 = math.sqrt(sum(v * v for v in vec1))
        norm2 = math.sqrt(sum(v * v for v in vec2))
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return dot / (norm1 * norm2)

    def search(self, query_text: str, n_results: int = 10) -> Dict[str, Any]:
        """Search indexed documents using a query string."""
        normalized_query = self._normalize_document(query_text)
        valid_docs = [
            (index, document)
            for index, document in enumerate(self.documents)
            if document and document.strip()
        ]
        if not valid_docs or not normalized_query.strip():
            return {"ids": [[]], "distances": [[]], "metadatas": [[]], "documents": [[]]}

        model = EmbeddingModel(max_features=5000)
        corpus = [document for _, document in valid_docs]
        try:
            model.fit([*corpus, normalized_query])
            query_embedding = model.encode_single(normalized_query)
            document_embeddings = model.encode(corpus)
        except ValueError:
            return {"ids": [[]], "distances": [[]], "metadatas": [[]], "documents": [[]]}

        similarities = []
        for i, corpus_vec in enumerate(document_embeddings):
            sim = self._cosine_similarity(query_embedding, corpus_vec)
            similarities.append((i, sim))

        similarities.sort(key=lambda item: item[1], reverse=True)
        top_similarities = similarities[:n_results]

        top_indices = [valid_docs[idx][0] for idx, _ in top_similarities]
        top_sim_scores = [sim for _, sim in top_similarities]

        return {
            "ids": [[self.ids[i] for i in top_indices]],
            "distances": [[1 - sim for sim in top_sim_scores]],
            "metadatas": [[self.metadatas[i] for i in top_indices]],
            "documents": [[self.documents[i] for i in top_indices]],
        }

    def delete_by_id(self, ids: List[str]):
        """Delete documents by ID."""
        for id_ in ids:
            if id_ in self.ids:
                idx = self.ids.index(id_)
                self.ids.pop(idx)
                self.embeddings.pop(idx)
                self.documents.pop(idx)
                self.metadatas.pop(idx)
        self._save()

    def count(self) -> int:
        """Get total number of indexed documents."""
        return len(self.ids)

    def sync_with_ids(self, valid_ids: List[str]) -> int:
        """Remove orphaned vectors that no longer match indexed metadata."""
        valid_id_set = set(valid_ids)
        keep_indexes = [index for index, id_ in enumerate(self.ids) if id_ in valid_id_set]
        removed = len(self.ids) - len(keep_indexes)
        if removed == 0:
            return 0

        self.ids = [self.ids[index] for index in keep_indexes]
        self.embeddings = [self.embeddings[index] for index in keep_indexes]
        self.documents = [self.documents[index] for index in keep_indexes]
        self.metadatas = [self.metadatas[index] for index in keep_indexes]
        self._save()
        return removed

    def get_diagnostics(self) -> Dict[str, Any]:
        """Return high-level integrity information about the vector store."""
        return {
            "vector_count": len(self.ids),
            "document_count": len(self.documents),
            "metadata_count": len(self.metadatas),
            "is_consistent": len(self.ids) == len(self.documents) == len(self.metadatas),
            "vectors_file": str(self.vectors_file),
        }

    def _save(self):
        data = {
            "version": 2,
            "ids": self.ids,
            "embeddings": self.embeddings,
            "documents": self.documents,
            "metadatas": self.metadatas,
        }
        atomic_write_text(self.vectors_file, json.dumps(data))

    def _load(self):
        if self.vectors_file.exists():
            try:
                payload = read_secure_text(
                    self.vectors_file,
                    max_bytes=int(config.get("security.max_vector_store_bytes", 50 * 1024 * 1024)),
                )
                data = json.loads(payload)
                ids = list(data.get("ids", []))
                embeddings = list(data.get("embeddings", []))
                documents = [self._normalize_document(doc) for doc in data.get("documents", [])]
                metadatas = list(data.get("metadatas", []))
                if not (len(ids) == len(embeddings) == len(documents) == len(metadatas)):
                    raise ValueError("Vector store lists are inconsistent")
                self.ids = ids
                self.embeddings = embeddings
                self.documents = documents
                self.metadatas = metadatas
            except (json.JSONDecodeError, ValueError) as exc:
                logger.warning("Vector store was invalid and has been reset: %s", exc)
                self.ids = []
                self.embeddings = []
                self.documents = []
                self.metadatas = []

    @staticmethod
    def _normalize_document(text: str) -> str:
        """Strip invisible/control characters that can skew search ranking."""
        normalized = unicodedata.normalize("NFKC", str(text or ""))
        cleaned = []
        for char in normalized:
            category = unicodedata.category(char)
            if category.startswith("C") and char not in {" ", "\n", "\t"}:
                continue
            cleaned.append(char)
        return " ".join("".join(cleaned).split())[
            : int(config.get("security.max_search_document_chars", 4000))
        ]
