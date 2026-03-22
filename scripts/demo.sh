#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Expected virtualenv python at $PYTHON_BIN"
  exit 1
fi

DEMO_DIR="$(mktemp -d "${TMPDIR:-/tmp}/lsiee-demo.XXXXXX")"
trap 'rm -rf "$DEMO_DIR"' EXIT

export LSIEE_DB_PATH="$DEMO_DIR/lsiee.db"
export LSIEE_VECTOR_DB_PATH="$DEMO_DIR/vectors"
export LSIEE_CONFIG_DIR="$DEMO_DIR/config"

mkdir -p "$DEMO_DIR/data"

cat > "$DEMO_DIR/data/release_notes.txt" <<'EOF'
Release checklist bug fixes deployment verification rollout notes.
EOF

cat > "$DEMO_DIR/data/sales.csv" <<'EOF'
region,revenue
west,200
east,100
north,150
EOF

cat > "$DEMO_DIR/data/report.json" <<'EOF'
{"report": {"summary": {"total": 450, "owner": "ops"}}}
EOF

echo "=== LSIEE DEMO ==="
echo

echo "1. Verifying installation"
"$PYTHON_BIN" "$ROOT_DIR/scripts/verify_installation.py"
echo

echo "2. Indexing sample files"
"$PYTHON_BIN" -m lsiee index "$DEMO_DIR/data" --no-progress
echo

echo "3. Showing status"
"$PYTHON_BIN" -m lsiee status
echo

echo "4. Running semantic search"
"$PYTHON_BIN" -m lsiee search "bug fixes"
echo

echo "5. Inspecting CSV and JSON"
"$PYTHON_BIN" -m lsiee inspect "$DEMO_DIR/data/sales.csv"
"$PYTHON_BIN" -m lsiee inspect "$DEMO_DIR/data/report.json" --json-path report.summary.total
echo

echo "6. Querying structured data"
"$PYTHON_BIN" -m lsiee query "$DEMO_DIR/data/sales.csv" "sum of revenue"
echo

echo "7. Collecting one monitoring iteration"
"$PYTHON_BIN" -m lsiee monitor --start --iterations 1 --interval 0.1
echo

echo "8. Seeding synthetic incident history"
"$PYTHON_BIN" - <<'PY'
import os
import sqlite3
import time
from pathlib import Path

from lsiee.storage.schemas import initialize_database
from lsiee.system_observability.detection import AlertManager
from lsiee.temporal_intelligence.correlation import EventCorrelator
from lsiee.temporal_intelligence.events import EventLogger

db_path = Path(os.environ["LSIEE_DB_PATH"])
demo_dir = db_path.parent
initialize_database(db_path).disconnect()
issue_time = time.time()

normal_rows = [
    (
        issue_time - (60 - index),
        5000 + index,
        "python",
        "/usr/bin/python",
        "python worker.py",
        10.0 + (index % 4),
        200.0 + index,
        5.0 + (index % 3) * 0.4,
        1000 + index * 20,
        900 + index * 18,
        "running",
        4 + (index % 2),
        issue_time - 7200,
        1,
    )
    for index in range(30)
]
incident_rows = [
    (
        issue_time - 5,
        8800,
        "backup.exe",
        "/usr/bin/backup.exe",
        "backup.exe --sync",
        91.0,
        1900.0,
        86.0,
        1100,
        950,
        "running",
        16,
        issue_time - 3600,
        1,
    ),
    (
        issue_time - 1,
        8800,
        "backup.exe",
        "/usr/bin/backup.exe",
        "backup.exe --sync",
        96.0,
        2100.0,
        89.0,
        1200,
        1000,
        "running",
        18,
        issue_time - 3600,
        1,
    ),
]

with sqlite3.connect(db_path) as conn:
    conn.executemany(
        """
        INSERT INTO process_snapshots
        (timestamp, pid, name, exe_path, cmdline, cpu_percent, memory_mb,
         memory_percent, io_read_bytes, io_write_bytes, status, num_threads,
         create_time, parent_pid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [*normal_rows, *incident_rows],
    )
    conn.commit()

alert_manager = AlertManager(db_path=db_path)
alert_manager.log_alerts(
    [
        {
            "type": "cpu_high",
            "source": "anomaly_detector",
            "severity": "WARNING",
            "message": "Synthetic demo CPU spike",
            "pid": 8800,
            "process_name": "backup.exe",
            "cpu_percent": 96.0,
            "timestamp": issue_time - 1,
        }
    ]
)

logger = EventLogger(db_path=db_path)
logger.log_events(
    [
        {
            "timestamp": issue_time - 10,
            "event_type": "index_completed",
            "source": "indexer",
            "severity": "INFO",
            "data": {"files_indexed": 2},
            "tags": ["file_intelligence"],
        },
        {
            "timestamp": issue_time - 8,
            "event_type": "cpu_high",
            "source": "monitor",
            "severity": "WARNING",
            "data": {"process": "backup.exe"},
            "tags": ["system_observability"],
        },
        {
            "timestamp": issue_time - 4,
            "event_type": "index_completed",
            "source": "indexer",
            "severity": "INFO",
            "data": {"files_indexed": 1},
            "tags": ["file_intelligence"],
        },
        {
            "timestamp": issue_time - 2,
            "event_type": "cpu_high",
            "source": "monitor",
            "severity": "WARNING",
            "data": {"process": "backup.exe"},
            "tags": ["system_observability"],
        },
    ]
)

correlator = EventCorrelator(db_path=db_path)
correlator.store_correlations(
    correlator.find_correlations(time_window=15.0, min_occurrences=2)
)

(demo_dir / "issue_time.txt").write_text(str(issue_time), encoding="utf-8")
PY
ISSUE_TIME="$(cat "$DEMO_DIR/issue_time.txt")"
echo

echo "9. Showing top CPU processes"
"$PYTHON_BIN" -m lsiee monitor --top-cpu --limit 5
echo

echo "10. Detecting anomalies from available history"
"$PYTHON_BIN" -m lsiee monitor --detect-anomalies
echo

echo "11. Showing alert history"
"$PYTHON_BIN" -m lsiee monitor --alert-history
echo

echo "12. Explaining a slowdown"
"$PYTHON_BIN" -m lsiee explain "system slowdown" --time "$ISSUE_TIME"
echo

echo "=== DEMO COMPLETE ==="
