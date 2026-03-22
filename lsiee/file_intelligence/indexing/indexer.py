"""Main indexing coordinator."""

import logging
from pathlib import Path
from typing import Optional

from lsiee.config import config, get_db_path
from lsiee.file_intelligence.indexing.scanner import DirectoryScanner
from lsiee.storage.metadata_db import FileRecord, MetadataDB

logger = logging.getLogger(__name__)


class Indexer:
    """Coordinates file indexing."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize indexer."""
        self.db_path = db_path or get_db_path()
        self.scanner = DirectoryScanner(
            excluded_patterns=config.get("index.excluded_patterns"),
            max_file_size_mb=config.get("index.max_file_size_mb", 50),
        )
        self._event_logger = None

    @property
    def event_logger(self):
        """Lazily initialize the event logger to avoid circular imports."""
        if self._event_logger is None:
            from lsiee.temporal_intelligence.events import EventLogger

            self._event_logger = EventLogger(self.db_path)
        return self._event_logger

    def index_directory(
        self, directory: Path, show_progress: bool = True, force: bool = False
    ) -> dict:
        """Index all files in directory.

        Args:
            directory: Directory to index
            show_progress: Show progress bar
            force: Re-index existing files even if unchanged

        Returns:
            Indexing statistics
        """
        from tqdm import tqdm

        logger.info(f"Indexing: {directory}")
        self.event_logger.log_event(
            event_type="index_started",
            source="file_indexer",
            data={"directory": str(directory), "force": force},
            tags=["file_intelligence", "indexing"],
        )

        try:
            self.scanner.reset_stats()
            files_metadata = list(self.scanner.scan(directory))

            indexed_count = 0
            updated_count = 0
            unchanged_count = 0
            error_count = 0

            with MetadataDB(self.db_path) as db:
                existing_by_path = db.get_files_by_paths(
                    metadata.path for metadata in files_metadata
                )
                iterator = (
                    tqdm(files_metadata, desc="Indexing") if show_progress else files_metadata
                )

                new_records = []
                updated_records = []

                for metadata in iterator:
                    try:
                        existing = existing_by_path.get(metadata.path)

                        record = FileRecord(
                            id=existing.id if existing else None,
                            path=metadata.path,
                            filename=metadata.filename,
                            extension=metadata.extension,
                            size_bytes=metadata.size_bytes,
                            modified_at=metadata.modified_at,
                            content_hash=metadata.content_hash,
                            index_status="pending",
                        )

                        if existing and not self._should_refresh(existing, record, force=force):
                            unchanged_count += 1
                            continue

                        if existing:
                            updated_records.append((existing.id, record))
                            updated_count += 1
                        else:
                            new_records.append(record)
                            indexed_count += 1

                        logger.debug("Queued %s for metadata indexing", metadata.filename)

                    except Exception as exc:
                        error_count += 1
                        logger.error("Error indexing %s: %s", metadata.path, exc)

                db.insert_files(new_records)
                db.update_file_records(updated_records)

            stats = {
                "files_discovered": len(files_metadata),
                "files_indexed": indexed_count,
                "files_updated": updated_count,
                "files_unchanged": unchanged_count,
                "errors": error_count,
                **self.scanner.get_stats(),
            }

            stats["files_skipped"] += unchanged_count
            self.event_logger.log_event(
                event_type="index_completed",
                source="file_indexer",
                data={"directory": str(directory), "stats": stats},
                tags=["file_intelligence", "indexing"],
            )
            logger.info(f"Indexing complete: {stats}")
            return stats
        except Exception as exc:
            self.event_logger.log_event(
                event_type="index_failed",
                source="file_indexer",
                data={"directory": str(directory), "error": str(exc)},
                severity="ERROR",
                tags=["file_intelligence", "indexing"],
            )
            raise

    @staticmethod
    def _should_refresh(existing: FileRecord, current: FileRecord, force: bool = False) -> bool:
        """Decide whether an existing file needs to be re-indexed."""
        if force:
            return True

        modified_delta = abs(existing.modified_at.timestamp() - current.modified_at.timestamp())
        return modified_delta > 1e-6 or existing.size_bytes != current.size_bytes
