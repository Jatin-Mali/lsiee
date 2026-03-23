# LSIEE User Guide

## Typical Workflow

### 1. Build The File Index

```bash
venv/bin/python -m lsiee index ~/Documents
venv/bin/python -m lsiee status
venv/bin/python -m lsiee verify
```

Use `--force` when you need a full metadata refresh.

### 2. Search By Meaning

```bash
venv/bin/python -m lsiee search "project rollout checklist"
```

LSIEE ranks results from indexed document text rather than filename matching.

### 3. Inspect Structured Files

```bash
venv/bin/python -m lsiee inspect reports/sales.csv
venv/bin/python -m lsiee inspect reports/workbook.xlsx --sheet Summary
venv/bin/python -m lsiee inspect reports/data.json --json-path report.summary.total
```

### 4. Query CSV Or Excel With Natural Language

```bash
venv/bin/python -m lsiee query reports/sales.csv "sum of revenue by region"
venv/bin/python -m lsiee query reports/sales.csv "filter revenue > 1000" --export filtered.csv
```

Supported operations include:

- filter
- sum
- average
- count
- group by
- max
- min
- sort

### Exporting Query Results

Use `--export` for structured query output, or use the dedicated `export` command to review all LSIEE-managed local data.

### 5. Collect Monitoring Data

```bash
venv/bin/python -m lsiee monitor --start --iterations 5 --interval 0.5
venv/bin/python -m lsiee monitor --top-cpu
venv/bin/python -m lsiee monitor --system
```

Use `--start` without `--iterations` to launch the detached daemon.

### 6. Review History And Detect Anomalies

```bash
venv/bin/python -m lsiee monitor --history-pid 1234 --hours 1
venv/bin/python -m lsiee monitor --timeline python --hours 24
venv/bin/python -m lsiee monitor --detect-anomalies
venv/bin/python -m lsiee monitor --alert-history
```

### 7. Explain An Incident

```bash
venv/bin/python -m lsiee explain "system slowdown"
venv/bin/python -m lsiee explain "memory pressure" --time 1710000000
```

The explanation engine combines:

- process metrics near the incident
- related events
- stored correlations
- historical recurrence

### 8. Export Or Delete Local LSIEE Data

```bash
venv/bin/python -m lsiee export --format json --output /tmp/lsiee-export.json
venv/bin/python -m lsiee export --format csv --output /tmp/lsiee-export.zip
venv/bin/python -m lsiee cleanup --dry-run
venv/bin/python -m lsiee delete-all-data --confirm DELETE
```

Review [PRIVACY.md](PRIVACY.md) for data categories and retention defaults.

## Troubleshooting

### No Search Results

- Run `index` again after adding files.
- Confirm the file type contains extractable text.
- Run `verify` to check for vector-store drift or database inconsistencies.
- Check `status` for failed files.

### Not Enough Data For Anomaly Detection

- Run `monitor --start --iterations ...` to seed history.
- Increase the collection window with `--hours`.

### No Explanation Evidence

- Ensure monitoring history or alerts exist near the incident time.
- Pass `--time` with a specific timestamp when diagnosing past issues.

### Isolated Testing

Use environment variables for disposable runs:

```bash
export LSIEE_DB_PATH=/tmp/lsiee-demo/lsiee.db
export LSIEE_VECTOR_DB_PATH=/tmp/lsiee-demo/vectors
export LSIEE_CONFIG_DIR=/tmp/lsiee-demo/config
```
