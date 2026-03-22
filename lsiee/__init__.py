"""
LSIEE - Local System Intelligence & Execution Engine

A local-first system intelligence platform.
"""

__version__ = "1.0.0"
__author__ = "LSIEE Contributors"

from . import file_intelligence, system_observability, temporal_intelligence

__all__ = [
    "__version__",
    "__author__",
    "file_intelligence",
    "system_observability",
    "temporal_intelligence",
]
