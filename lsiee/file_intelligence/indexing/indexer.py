"""Main indexing coordinator."""

from pathlib import Path
from typing import Optional
import logging

from lsiee.file_intelligence.indexing.scanner import DirectoryScanner
from lsiee.storage.metadata_db import MetadataDB, FileRecord
from lsiee.config import config

logger = logging.getLogger(__name__)


class Indexer:
    """Coordinates file indexing."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """Initialize indexer."""
        if db_path is None:
            db_path = Path.home() / ".lsiee" / "lsiee.db"
        
        self.db_path = db_path
        self.scanner = DirectoryScanner(
            excluded_patterns=config.get('index.excluded_patterns'),
            max_file_size_mb=config.get('index.max_file_size_mb', 50)
        )
    
    def index_directory(self, directory: Path, show_progress: bool = True) -> dict:
        """Index all files in directory.
        
        Args:
            directory: Directory to index
            show_progress: Show progress bar
            
        Returns:
            Indexing statistics
        """
        from tqdm import tqdm
        
        logger.info(f"Indexing: {directory}")
        
        self.scanner.reset_stats()
        files_metadata = list(self.scanner.scan(directory))
        
        indexed_count = 0
        updated_count = 0
        error_count = 0
        
        with MetadataDB(self.db_path) as db:
            iterator = tqdm(files_metadata, desc="Indexing") if show_progress else files_metadata
            
            for metadata in iterator:
                try:
                    existing = db.get_file_by_path(metadata.path)
                    
                    if existing:
                        updated_count += 1
                        continue
                    
                    record = FileRecord(
                        id=None,
                        path=metadata.path,
                        filename=metadata.filename,
                        extension=metadata.extension,
                        size_bytes=metadata.size_bytes,
                        modified_at=metadata.modified_at,
                        content_hash=metadata.content_hash,
                        index_status='pending'
                    )
                    
                    file_id = db.insert_file(record)
                    indexed_count += 1
                    
                    logger.debug(f"Indexed: {metadata.filename} (ID: {file_id})")
                
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error indexing {metadata.path}: {e}")
        
        stats = {
            'files_discovered': len(files_metadata),
            'files_indexed': indexed_count,
            'files_updated': updated_count,
            'errors': error_count,
            **self.scanner.get_stats()
        }
        
        logger.info(f"Indexing complete: {stats}")
        return stats