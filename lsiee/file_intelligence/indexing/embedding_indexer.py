"""Index files for semantic search."""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from lsiee.config import get_db_path, get_vector_db_path
from lsiee.file_intelligence.search.embeddings import EmbeddingModel
from lsiee.file_intelligence.search.text_extractor import TextExtractor
from lsiee.storage.metadata_db import MetadataDB
from lsiee.storage.vector_db import VectorDB

logger = logging.getLogger(__name__)


class EmbeddingIndexer:
    """Index files with TF-IDF-backed search data."""

    def __init__(self, db_path: Optional[Path] = None, vector_db_path: Optional[Path] = None):
        """Initialize the embedding indexer."""
        self.embedding_model = EmbeddingModel()
        self.text_extractor = TextExtractor()
        self.db_path = db_path or get_db_path()
        self.vector_db = VectorDB(vector_db_path or get_vector_db_path())
        self.metadata_db = MetadataDB(self.db_path)

    def index_file(self, file_path: str) -> bool:
        """Index a single file."""
        filepath = Path(file_path)
        text = self.text_extractor.extract(filepath)
        if not text:
            logger.debug("No text extracted from %s", filepath)
            return False

        self.embedding_model.fit([text])
        embedding = self.embedding_model.encode_single(text)
        stat = filepath.stat()

        self.vector_db.add_embeddings(
            ids=[file_path],
            embeddings=[embedding],
            documents=[text[:1000]],
            metadatas=[
                {
                    "filename": filepath.name,
                    "extension": filepath.suffix,
                    "size": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                }
            ],
        )

        logger.info("Indexed: %s", filepath.name)
        return True

    def index_all_pending(self) -> int:
        """Index all files currently marked as pending."""
        with self.metadata_db as db:
            files = db.get_all_files(status="pending")

        logger.info("Indexing %s files with embeddings", len(files))

        candidates = []
        for file_record in files:
            filepath = Path(file_record.path)
            text = self.text_extractor.extract(filepath)
            if not text:
                with self.metadata_db as db:
                    db.update_file_status(file_record.id, "failed", "No searchable text extracted")
                continue
            candidates.append((file_record, filepath, text))

        if not candidates:
            return 0

        texts = [text for _, _, text in candidates]
        self.embedding_model.fit(texts)
        embeddings = self.embedding_model.encode(texts)

        ids: List[str] = []
        documents: List[str] = []
        metadatas = []

        for (file_record, filepath, text), embedding in zip(candidates, embeddings):
            ids.append(file_record.path)
            documents.append(text[:1000])
            metadatas.append(
                {
                    "filename": filepath.name,
                    "extension": filepath.suffix,
                    "size": filepath.stat().st_size,
                    "modified_at": file_record.modified_at.isoformat(),
                }
            )
            logger.debug(
                "Prepared semantic index entry for %s (%s dims)", filepath.name, len(embedding)
            )

        self.vector_db.add_embeddings(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )

        with self.metadata_db as db:
            for file_record, _, _ in candidates:
                db.update_file_status(file_record.id, "indexed")

        logger.info("Indexed %s files", len(candidates))
        return len(candidates)
