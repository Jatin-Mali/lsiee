# LSIEE Quick Start

## Activate Virtual Environment

```bash
cd lsiee
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

## Verify Installation

```bash
venv/bin/python scripts/verify_installation.py
venv/bin/python -m lsiee --help
venv/bin/python -m lsiee status
```

## Development Workflow

```bash
# Run tests
venv/bin/pytest

# Format code
venv/bin/python -m black lsiee tests
venv/bin/python -m isort lsiee tests

# Type checking
venv/bin/python -m mypy lsiee/

# Linting
venv/bin/python -m pylint lsiee/
```

## Current Commands

- `venv/bin/python -m lsiee index <directory>`
- `venv/bin/python -m lsiee search "query"`
- `venv/bin/python -m lsiee inspect <file>`
- `venv/bin/python -m lsiee query <file> "natural language query"`
- `venv/bin/python -m lsiee monitor --top-cpu`
- `venv/bin/python -m lsiee monitor --detect-anomalies`

Phase 7 adds integration coverage, indexing fixes, and lightweight performance verification.
