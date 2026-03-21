#!/usr/bin/env python3
"""Initialize LSIEE databases."""

import sqlite3
from pathlib import Path

def init_databases():
    """Initialize all LSIEE databases."""
    data_dir = Path.home() / ".lsiee"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize metadata database
    db_path = data_dir / "lsiee.db"
    conn = sqlite3.connect(db_path)
    
    # Create tables (basic schema for now)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            filename TEXT NOT NULL,
            indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print(f"✓ Initialized database: {db_path}")

if __name__ == '__main__':
    init_databases()
