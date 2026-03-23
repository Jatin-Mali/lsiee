"""Index files for semantic search."""

import logging
from pathlib import Path
from typing import List, Optional

from lsiee.config import config, get_db_path, get_vector_db_path
from lsiee.file_intelligence.search.embeddings import EmbeddingModel
from lsiee.file_intelligence.search.text_extractor import TextExtractor
from lsiee.security import PathSecurityError, ensure_safe_file
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
        try:
            filepath = ensure_safe_file(Path(file_path))
        except PathSecurityError:
            logger.warning("Skipping unsafe search index path: %s", file_path)
            return False

        text = self.text_extractor.extract(filepath)
        if not text:
            logger.debug("No text extracted from %s", filepath)
            return False

        self.vector_db.add_embeddings(
            ids=[file_path],
            embeddings=[[]],
            documents=[
                (
                    f"{filepath.name} "
                    f"{text[:int(config.get('security.max_search_document_chars', 4000))]}"
                )
            ],
            metadatas=[
                {
                    "filename": filepath.name,
                    "extension": filepath.suffix,
                    "size": filepath.stat().st_size,
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
        skipped_ids: List[str] = []
        failed_records: List[int] = []
        skipped_records: List[int] = []
        for file_record in files:
            try:
                filepath = ensure_safe_file(Path(file_record.path))
            except PathSecurityError:
                with self.metadata_db as db:
                    db.update_file_status(file_record.id, "failed", "File is missing or unsafe")
                failed_records.append(file_record.id)
                skipped_ids.append(file_record.path)
                continue

            try:
                text = self.text_extractor.extract(filepath)
            except Exception:
                text = None

            if not text or len(text.strip()) < int(
                config.get("security.min_search_document_chars", 10)
            ):
                skipped_records.append(file_record.id)
                skipped_ids.append(file_record.path)
                continue

            document = (
                f"{filepath.name} {filepath.suffix.lstrip('.')} "
                f"{text[:int(config.get('security.max_search_document_chars', 4000))]}"
            )

            candidates.append((file_record, filepath, document))

        texts = [text for _, _, text in candidates]
        embeddings = self.embedding_model.encode(texts) if texts else []

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

        if ids:
            self.vector_db.add_embeddings(
                ids=ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas,
            )
        if skipped_ids:
            self.vector_db.delete_by_id(skipped_ids)

        with self.metadata_db as db:
            for file_record, _, _ in candidates:
                db.update_file_status(file_record.id, "indexed")
            for file_id in skipped_records:
                db.update_file_status(file_id, "skipped", "File is unsupported or non-text")
            for file_id in failed_records:
                db.update_file_status(file_id, "failed", "File is missing or unsafe")
            indexed_paths = [record.path for record in db.get_all_files(status="indexed")]

        self.vector_db.sync_with_ids(indexed_paths)

        logger.info("Indexed %s files", len(candidates))
        return len(candidates)
