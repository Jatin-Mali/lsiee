# LSIEE Development Guide

## Environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Project Checks

```bash
venv/bin/python scripts/verify_installation.py
venv/bin/python -m lsiee verify
venv/bin/pytest -q
venv/bin/python -m black --check lsiee tests scripts
venv/bin/python -m isort --check-only lsiee tests scripts
venv/bin/python -m flake8 lsiee tests scripts
```

## Common Workflows

### Run A Focused Test File

```bash
venv/bin/pytest tests/integration/test_complete_workflow.py -q
```

### Reformat The Repository

```bash
venv/bin/python -m black lsiee tests scripts
venv/bin/python -m isort lsiee tests scripts
```

### Validate Packaging And CLI

```bash
venv/bin/python -m lsiee --help
venv/bin/python -m lsiee status
venv/bin/python -m lsiee verify
```

## Coding Standards

- Keep the implementation local-first with no cloud dependency requirement.
- Prefer deterministic tests with temp directories and isolated DB paths.
- Use SQLite helpers from `lsiee.storage.schemas` instead of ad-hoc connection setup.
- Keep CLI-facing failures readable and actionable.

## Release Workflow

```bash
git status --short
venv/bin/pip-audit
venv/bin/python scripts/verify_installation.py
venv/bin/python -m lsiee verify
venv/bin/pytest -q
venv/bin/python -m black --check lsiee tests scripts
venv/bin/python -m isort --check-only lsiee tests scripts
venv/bin/python -m flake8 lsiee tests scripts
git add .
git commit -m "LSIEE v1.0.0 - final integration"
git tag -a v1.0.0 -m "LSIEE v1.0.0 - Final Release"
git push origin main --tags
```

## Security Release Notes

- Keep dependency versions pinned in `requirements.txt`.
- Validate `lsiee export`, `lsiee cleanup`, and `lsiee delete-all-data` in an isolated temp workspace before release.
- Review [SECURITY.md](SECURITY.md) and [PRIVACY.md](PRIVACY.md) whenever the data model or monitoring surface changes.
