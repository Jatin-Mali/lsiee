# LSIEE API Reference

## CLI Entry Point

- `lsiee.cli.main()`
  - Commands: `index`, `status`, `search`, `inspect`, `query`, `monitor`, `explain`

## File Intelligence

### Indexing

- `DirectoryScanner.scan(directory)`
  - Walk a directory tree and yield `FileMetadata`
- `extract_metadata(filepath, calculate_hash=False)`
  - Return filesystem metadata for one file
- `Indexer.index_directory(directory, show_progress=True, force=False)`
  - Discover, classify, and persist file metadata
- `EmbeddingIndexer.index_all_pending()`
  - Refresh semantic-search documents for indexed files

### Search

- `SemanticSearch.search(query, max_results=10)`
  - Return ranked semantic matches
- `TextExtractor.extract_text(filepath)`
  - Convert supported files into search-ready text

### Structured Data

- `StructuredDataParser.parse_csv(filepath)`
- `StructuredDataParser.parse_excel(filepath, sheet_name=None)`
- `StructuredDataParser.parse_json(filepath)`
- `StructuredDataParser.extract_json_path(filepath, json_path)`
- `SchemaDetector.detect_csv_schema(filepath)`
- `SchemaDetector.detect_excel_schema(filepath, sheet_name=None)`
- `QueryExecutor.execute_query(filepath, query)`
- `QueryExecutor.execute_query_safe(filepath, query, timeout=30)`
- `ResultFormatter.format_table(rows)`
- `ResultFormatter.export_to_file(payload, export_path, format="csv")`

## System Observability

### Monitoring

- `ProcessMonitor.capture_snapshot(cpu_interval=0.0)`
- `ProcessMonitor.get_top_cpu(n=10)`
- `ProcessMonitor.get_top_memory(n=10)`
- `ProcessMonitor.get_process_by_name(name)`
- `SystemMetrics.get_all_metrics()`
- `MonitoringDaemon.start(blocking=False, iterations=None)`
- `MonitoringDaemon.stop()`
- `ProcessHistory.get_recent_history(hours=24, limit=100)`
- `ProcessHistory.get_process_history(pid, start_time, end_time)`
- `ProcessHistory.get_cpu_timeline(process_name, hours=24)`

### Detection

- `AnomalyDetector.fit(process_data)`
- `AnomalyDetector.predict(process_snapshot)`
- `RealtimeAnomalyDetector.update(snapshot)`
- `AlertManager.check_thresholds(metrics, prediction=None)`
- `AlertManager.log_alert(alert)`
- `AlertManager.log_alerts(alerts)`
- `AlertManager.get_recent_alerts(hours=24, limit=20)`

## Temporal Intelligence

### Events And Correlation

- `EventLogger.log_event(event_type, source, data, severity="INFO", ...)`
- `EventLogger.log_events(events)`
- `EventLogger.get_events(event_type=None, source=None, ...)`
- `EventCorrelator.find_correlations(time_window=60.0, min_support=0.01, min_occurrences=2)`
- `EventCorrelator.store_correlations(correlations)`
- `EventCorrelator.get_stored_correlations(min_lift=1.0, limit=20)`
- `PatternDetector.detect_patterns(events)`

### Explanation

- `RootCauseAnalyzer.explain_issue(issue, timestamp)`
- `RootCauseAnalyzer.explain_slowdown(timestamp)`
- `EvidenceGatherer.gather_evidence(issue_type, timestamp, window_seconds=None)`
- `RecommendationEngine.recommend(issue_type, context)`
- `parse_issue_timestamp(raw_timestamp)`

## Storage

- `initialize_database(db_path)`
- `configure_connection(conn)`
- `MetadataDB.insert_file(file_record)`
- `MetadataDB.insert_files(file_records)`
- `MetadataDB.get_file_by_path(path)`
- `MetadataDB.get_files_by_paths(paths)`
- `MetadataDB.update_file_record(file_id, file_record)`
- `MetadataDB.get_stats()`

## Configuration

- `Config.get(key, default=None)`
- `Config.set(key, value)`
- `get_db_path()`
- `get_vector_db_path()`
- `config`
