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
lsiee --help
lsiee status
python scripts/verify_installation.py
```

## Development Workflow

```bash
# Run tests
pytest

# Format code
black lsiee/
isort lsiee/

# Type checking
mypy lsiee/

# Linting
pylint lsiee/
```

## Start Coding

Week 1: Implement file indexing in `lsiee/file_intelligence/indexing/`

Good luck building LSIEE! 🚀
