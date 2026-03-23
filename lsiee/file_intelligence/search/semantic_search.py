"""Semantic search implementation."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from lsiee.config import config, get_db_path, get_vector_db_path
from lsiee.security import validate_positive_int, validate_query_text
from lsiee.storage.vector_db import VectorDB

logger = logging.getLogger(__name__)


class SemanticSearch:
    """Semantic file search."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        vector_db_path: Optional[Path] = None,
    ):
        """Initialize search engine."""
        self.db_path = db_path or get_db_path()
        self.vector_db = VectorDB(vector_db_path or get_vector_db_path())

    def search(self, query: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """Search for files matching a query."""
        try:
            query = validate_query_text(
                query,
                max_length=int(config.get("security.max_query_length", 500)),
                max_conditions=int(config.get("security.max_query_conditions", 3)),
            )
            max_results = validate_positive_int(max_results, name="max_results", maximum=1000)
        except ValueError:
            return []

        logger.info("Searching for: %s", query)

        results = self.vector_db.search(query_text=query, n_results=max_results)
        formatted_results = []

        ids = results.get("ids", [[]])
        if not ids or not ids[0]:
            return formatted_results

        for i in range(len(ids[0])):
            formatted_results.append(
                {
                    "file_path": ids[0][i],
                    "similarity": 1 - results["distances"][0][i],
                    "metadata": results["metadatas"][0][i],
                    "snippet": " ".join(results["documents"][0][i].split())[:200],
                }
            )

        filtered_results = [
            result
            for result in formatted_results
            if result["similarity"] >= config.get("search.min_confidence_threshold", 0.0)
        ]
        return self.rerank_results(filtered_results, query)

    def rerank_results(
        self,
        results: List[Dict[str, Any]],
        query: str,
    ) -> List[Dict[str, Any]]:
        """Re-rank results using multiple signals."""
        for result in results:
            score = result["similarity"] * 0.7

            modified = result["metadata"].get("modified_at")
            if modified:
                if isinstance(modified, str):
                    try:
                        modified = datetime.fromisoformat(modified)
                    except ValueError:
                        modified = None

                if isinstance(modified, datetime):
                    days_old = max((datetime.now() - modified).days, 0)
                    recency_score = max(0, 1 - (days_old / 365))
                    score += recency_score * 0.2

            extension = result["metadata"].get("extension", "")
            if extension in [".py", ".js", ".md"] and any(
                word in query.lower() for word in ["code", "function", "bug", "fix"]
            ):
                score += 0.1

            result["final_score"] = score

        results.sort(key=lambda item: item.get("final_score", item["similarity"]), reverse=True)
        return results
