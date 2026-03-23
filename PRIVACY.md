# LSIEE Privacy Guide

LSIEE is local-first. It stores data on the current machine and does not upload indexed content, process history, or event timelines to external services.

## Data Collected

- `files`: indexed file paths, names, extensions, sizes, timestamps, and indexing state
- `search_index`: local searchable document text and semantic-search metadata
- `process_snapshots`: process name, CPU, memory, status, PID, and thread counts
- `events`: local alert, monitoring, and indexing events used for correlation and explanations
- `config`: local LSIEE settings

## Default Storage Locations

- Database: `~/.lsiee/lsiee.db`
- Vector store: `~/.lsiee/vectors/`
- Config: `~/.lsiee/config.yaml`
- Logs: `~/.lsiee/logs/`

Environment overrides:

- `LSIEE_DB_PATH`
- `LSIEE_VECTOR_DB_PATH`
- `LSIEE_CONFIG_DIR`
- `LSIEE_DATA_DIR`

## Retention

- Process snapshots: 30 days by default
- Events: 90 days by default
- File metadata and search index: retained until the source file is deleted or the user removes LSIEE data

Config keys:

- `retention.process_snapshots_days`
- `retention.events_days`
- `index.excluded_directories`
- `index.excluded_patterns`

## User Controls

- Export all local data:
  - `venv/bin/python -m lsiee export --format json --output /tmp/lsiee-export.json`
  - `venv/bin/python -m lsiee export --format csv --output /tmp/lsiee-export.zip`
- Preview cleanup:
  - `venv/bin/python -m lsiee cleanup --dry-run`
- Delete aged monitoring/event data:
  - `venv/bin/python -m lsiee cleanup --type events --older-than 30 --yes`
- Delete all LSIEE-managed local data:
  - `venv/bin/python -m lsiee delete-all-data --confirm DELETE`

## Privacy Notes

- Monitoring defaults to the current user only.
- Command lines and executable paths are not stored by default.
- Sensitive values are redacted from logs and event payloads.
- Terminal output is sanitized to avoid control-sequence injection from untrusted filenames or content.
