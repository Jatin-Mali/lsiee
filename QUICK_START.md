# LSIEE Quick Start

## 1. Set Up The Environment

```bash
git clone https://github.com/yourusername/lsiee.git
cd lsiee
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## 2. Verify The Installation

```bash
venv/bin/python scripts/verify_installation.py
venv/bin/python -m lsiee --help
```

## 3. Index And Search Files

```bash
venv/bin/python -m lsiee index ~/Documents
venv/bin/python -m lsiee status
venv/bin/python -m lsiee search "release checklist"
```

## 4. Inspect And Query Structured Data

```bash
venv/bin/python -m lsiee inspect data/sales.csv
venv/bin/python -m lsiee inspect data/report.json --json-path report.summary.total
venv/bin/python -m lsiee query data/sales.csv "sum of revenue by region"
```

## 5. Monitor The System

```bash
venv/bin/python -m lsiee monitor --start --iterations 1 --interval 0.1
venv/bin/python -m lsiee monitor --top-cpu
venv/bin/python -m lsiee monitor --system
venv/bin/python -m lsiee monitor --detect-anomalies
venv/bin/python -m lsiee monitor --alert-history
```

## 6. Explain An Incident

```bash
venv/bin/python -m lsiee explain "system slowdown"
venv/bin/python -m lsiee explain "cpu high" --time 1710000000
```

## 7. Run The Full Validation Suite

```bash
venv/bin/pytest -q
venv/bin/python -m black --check lsiee tests scripts
venv/bin/python -m isort --check-only lsiee tests scripts
venv/bin/python -m flake8 lsiee tests scripts
```

## More Detail

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [API_REFERENCE.md](API_REFERENCE.md)
- [USER_GUIDE.md](USER_GUIDE.md)
- [DEVELOPMENT.md](DEVELOPMENT.md)
- [PERFORMANCE.md](PERFORMANCE.md)
