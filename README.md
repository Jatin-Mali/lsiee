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

# Run setup script
./scripts/setup.sh
```

## Quick Start

```bash
# Activate virtual environment
source venv/bin/activate

# Index your files
lsiee index ~/Documents

# Search semantically
lsiee search "quarterly budget reports"

# Inspect structured data
lsiee inspect data.xlsx

# Extract data with natural language
lsiee extract "total revenue by region" sales.xlsx

# Monitor system
lsiee monitor

# Analyze behavior
lsiee explain "why is system slow"
```

## Documentation

- [User Guide](docs/user_guide.md)
- [Developer Guide](docs/developer_guide.md)
- [Architecture](docs/architecture.md)

## License

MIT License - See [LICENSE](LICENSE) for details.
