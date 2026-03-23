"""Extract text from various file types."""

import logging
from pathlib import Path
from typing import List, Optional

from lsiee.config import config
from lsiee.security import PathSecurityError, read_secure_text

logger = logging.getLogger(__name__)


class TextExtractor:
    """Extract searchable text from files."""

    def extract(self, filepath: Path) -> Optional[str]:
        """Extract text from file.

        Args:
            filepath: Path to file

        Returns:
            Extracted text or None
        """
        extension = filepath.suffix.lower()

        try:
            if extension in [".txt", ".md", ".py", ".js", ".json", ".csv"]:
                return self._extract_plain_text(filepath)
            else:
                logger.debug(f"Unsupported file type: {extension}")
                return None

        except (PathSecurityError, OSError) as exc:
            logger.warning("Could not extract text from %s: %s", filepath.name, exc)
            return None

    def _extract_plain_text(self, filepath: Path) -> str:
        """Extract from plain text file."""
        return read_secure_text(
            filepath,
            max_bytes=int(config.get("security.max_text_extract_bytes", 1024 * 1024)),
        )

    def chunk_text(self, text: str, chunk_size: int = 512) -> List[str]:
        """Split text into chunks.

        Args:
            text: Full text
            chunk_size: Characters per chunk

        Returns:
            List of text chunks
        """
        chunks = []
        for i in range(0, len(text), chunk_size):
            chunk = text[i : i + chunk_size]
            if chunk.strip():
                chunks.append(chunk)
        return chunks
