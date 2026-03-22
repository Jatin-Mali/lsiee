"""Persistent document store for TF-IDF search."""

import json
import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

from lsiee.config import get_vector_db_path
from lsiee.file_intelligence.search.embeddings import EmbeddingModel

logger = logging.getLogger(__name__)


class VectorDB:
    """JSON-backed storage for searchable file documents."""

    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            db_path = get_vector_db_path()

        self.db_path = db_path
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
            if id_ in self.ids:
                idx = self.ids.index(id_)
                self.ids.pop(idx)
                self.embeddings.pop(idx)
                self.documents.pop(idx)
                self.metadatas.pop(idx)

            self.ids.append(id_)
            self.embeddings.append(emb)
            self.documents.append(doc)
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
        if not self.documents or not query_text.strip():
            return {"ids": [[]], "distances": [[]], "metadatas": [[]], "documents": [[]]}

        model = EmbeddingModel(max_features=5000)
        model.fit([*self.documents, query_text])

        query_embedding = model.encode_single(query_text)
        document_embeddings = model.encode(self.documents)

        similarities = []
        for i, corpus_vec in enumerate(document_embeddings):
            sim = self._cosine_similarity(query_embedding, corpus_vec)
            similarities.append((i, sim))

        similarities.sort(key=lambda item: item[1], reverse=True)
        top_similarities = similarities[:n_results]

        top_indices = [idx for idx, _ in top_similarities]
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

    def _save(self):
        data = {
            "ids": self.ids,
            "embeddings": self.embeddings,
            "documents": self.documents,
            "metadatas": self.metadatas,
        }
        with open(self.vectors_file, "w", encoding="utf-8") as f:
            json.dump(data, f)

    def _load(self):
        if self.vectors_file.exists():
            with open(self.vectors_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.ids = data.get("ids", [])
            self.embeddings = data.get("embeddings", [])
            self.documents = data.get("documents", [])
            self.metadatas = data.get("metadatas", [])
