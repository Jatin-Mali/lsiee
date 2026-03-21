"""Metadata database access layer."""

import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass


@dataclass
class FileRecord:
    """File record dataclass."""
    id: Optional[int]
    path: str
    filename: str
    extension: Optional[str]
    size_bytes: int
    modified_at: datetime
    indexed_at: Optional[datetime] = None
    content_hash: Optional[str] = None
    index_status: str = 'pending'


class MetadataDB:
    """Database interface for file metadata."""
    
    def __init__(self, db_path: Path):
        """Initialize database connection.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
    
    def connect(self):
        """Connect to database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
    
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def insert_file(self, file_record: FileRecord) -> int:
        """Insert a new file record.
        
        Args:
            file_record: File record to insert
            
        Returns:
            ID of inserted record
        """
        cursor = self.conn.execute('''
            INSERT INTO files (path, filename, extension, size_bytes, modified_at, 
                             content_hash, index_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            file_record.path,
            file_record.filename,
            file_record.extension,
            file_record.size_bytes,
            file_record.modified_at.timestamp(),
            file_record.content_hash,
            file_record.index_status
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def get_file_by_path(self, path: str) -> Optional[FileRecord]:
        """Get file record by path.
        
        Args:
            path: File path
            
        Returns:
            FileRecord if found, None otherwise
        """
        cursor = self.conn.execute(
            'SELECT * FROM files WHERE path = ?',
            (path,)
        )
        row = cursor.fetchone()
        
        if row:
            return FileRecord(
                id=row['id'],
                path=row['path'],
                filename=row['filename'],
                extension=row['extension'],
                size_bytes=row['size_bytes'],
                modified_at=datetime.fromtimestamp(row['modified_at']),
                content_hash=row['content_hash'],
                index_status=row['index_status']
            )
        return None
    
    def get_all_files(self, status: Optional[str] = None) -> List[FileRecord]:
        """Get all file records.
        
        Args:
            status: Optional filter by index_status
            
        Returns:
            List of FileRecords
        """
        if status:
            cursor = self.conn.execute(
                'SELECT * FROM files WHERE index_status = ? ORDER BY indexed_at DESC',
                (status,)
            )
        else:
            cursor = self.conn.execute(
                'SELECT * FROM files ORDER BY indexed_at DESC'
            )
        
        records = []
        for row in cursor.fetchall():
            records.append(FileRecord(
                id=row['id'],
                path=row['path'],
                filename=row['filename'],
                extension=row['extension'],
                size_bytes=row['size_bytes'],
                modified_at=datetime.fromtimestamp(row['modified_at']),
                content_hash=row['content_hash'],
                index_status=row['index_status']
            ))
        
        return records
    
    def update_file_status(self, file_id: int, status: str, error: Optional[str] = None):
        """Update file indexing status.
        
        Args:
            file_id: File ID
            status: New status
            error: Error message if status is 'failed'
        """
        self.conn.execute('''
            UPDATE files 
            SET index_status = ?, index_error = ?
            WHERE id = ?
        ''', (status, error, file_id))
        self.conn.commit()
    
    def get_file_count(self) -> int:
        """Get total number of indexed files.
        
        Returns:
            Number of files
        """
        cursor = self.conn.execute('SELECT COUNT(*) as count FROM files')
        return cursor.fetchone()['count']
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics.
        
        Returns:
            Dictionary with statistics
        """
        cursor = self.conn.execute('''
            SELECT 
                COUNT(*) as total_files,
                SUM(size_bytes) as total_size,
                COUNT(CASE WHEN index_status = 'indexed' THEN 1 END) as indexed_count,
                COUNT(CASE WHEN index_status = 'failed' THEN 1 END) as failed_count
            FROM files
        ''')
        
        row = cursor.fetchone()
        return {
            'total_files': row['total_files'],
            'total_size_bytes': row['total_size'] or 0,
            'indexed_count': row['indexed_count'],
            'failed_count': row['failed_count']
        }