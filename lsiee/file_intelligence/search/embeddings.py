"""Text embedding using TF-IDF."""

import logging
from typing import List, Optional

from sklearn.feature_extraction.text import TfidfVectorizer

logger = logging.getLogger(__name__)


class EmbeddingModel:
    """TF-IDF based embedding model."""

    def __init__(self, model_name: Optional[str] = None, max_features: int = 5000):
        """Initialize the TF-IDF model.

        Args:
            model_name: Ignored for compatibility with the original transformer API.
            max_features: Maximum vocabulary size.
        """
        if model_name:
            logger.info("Ignoring transformer model '%s' and using TF-IDF", model_name)

        self.max_features = max_features
        self.vectorizer = TfidfVectorizer(
            max_features=max_features,
            stop_words="english",
            token_pattern=r"(?u)\b\w\w+\b",
        )
        self.is_fitted = False
        self.embedding_dim = 0

    def fit(self, texts: List[str]):
        """Fit the vectorizer on a corpus."""
        if not texts:
            return

        self.vectorizer.fit(texts)
        self.embedding_dim = len(self.vectorizer.get_feature_names_out())
        self.is_fitted = True
        logger.info(
            "Fitted TF-IDF on %s documents (vocab size %s)",
            len(texts),
            self.embedding_dim,
        )

    def encode(self, texts: List[str]) -> List[List[float]]:
        """Transform text into dense TF-IDF vectors."""
        if not texts:
            return []

        if not self.is_fitted:
            self.fit(texts)

        matrix = self.vectorizer.transform(texts)
        return matrix.toarray().tolist()

    def encode_single(self, text: str) -> List[float]:
        """Transform a single text value into a TF-IDF vector."""
        return self.encode([text])[0]
