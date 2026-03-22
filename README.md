# LSIEE - Local System Intelligence & Execution Engine

A local-first system intelligence platform that provides:

1. **File Intelligence** - Semantic search, structured data parsing, natural language data extraction
2. **System Observability** - Process monitoring, anomaly detection, resource tracking
3. **Temporal Intelligence** - Event logging, behavior correlation, root cause analysis

## Features

- 🔍 **Semantic File Search** - Find files by meaning, not filename
- 📊 **Structured Data Parsing** - Auto-detect schemas in CSV/Excel/JSON
- 💬 **Natural Language Queries** - Extract data using plain English
- 🖥️ **Process Monitoring** - Track system processes and resource usage
- 🚨 **Anomaly Detection** - Automatically detect performance spikes
- 📈 **Temporal Analysis** - Correlate events and explain system behavior
- 🔒 **Privacy-First** - 100% local processing, zero cloud dependency

## Installation

```bash
# Clone repository
git clone https://github.com/yourusername/lsiee.git
cd lsiee

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

```bash
# Index your files
lsiee index ~/Documents

# Search semantically
lsiee search "quarterly budget reports"

# Inspect structured data
lsiee inspect data.xlsx

# Query structured data with natural language
lsiee query sales.xlsx "total revenue by region"

# Collect and inspect process activity
lsiee monitor --top-cpu
lsiee monitor --start --iterations 1

# Detect anomalies from stored history
lsiee monitor --detect-anomalies
lsiee monitor --alert-history
```

## Testing

```bash
venv/bin/python scripts/verify_installation.py
venv/bin/pytest -q
```

## Current Scope

- File indexing and semantic search
- Structured CSV/Excel/JSON inspection
- Natural-language tabular querying
- Process monitoring and stored history
- Anomaly detection with persisted alerts

## License

MIT License - See [LICENSE](LICENSE) for details.
