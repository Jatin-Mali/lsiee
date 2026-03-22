"""LSIEE Command Line Interface."""

from datetime import datetime, timedelta
import logging
from pathlib import Path

import click
from rich.console import Console
from rich.json import JSON
from rich.table import Table

from lsiee.config import config, get_db_path, get_vector_db_path
from lsiee.file_intelligence.data_extraction.parsers import StructuredDataParser
from lsiee.file_intelligence.data_extraction.query_executor import QueryExecutor
from lsiee.file_intelligence.data_extraction.result_formatter import ResultFormatter
from lsiee.file_intelligence.data_extraction.schema_detector import SchemaDetector
from lsiee.file_intelligence.indexing.embedding_indexer import EmbeddingIndexer
from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.file_intelligence.search.semantic_search import SemanticSearch
from lsiee.storage.metadata_db import MetadataDB
from lsiee.storage.schemas import initialize_database
from lsiee.system_observability.detection import AlertManager, AnomalyDetector
from lsiee.system_observability.monitoring import (
    MonitoringDaemon,
    ProcessHistory,
    ProcessMonitor,
    SystemMetrics,
    get_daemon_status,
    spawn_background_daemon,
    stop_background_daemon,
)

console = Console()
logging.basicConfig(level=logging.INFO)


@click.group()
@click.version_option(version="0.1.0")
def main():
    """LSIEE - Local System Intelligence & Execution Engine

    A local-first system intelligence platform for understanding
    and interacting with your operating system.
    """
    pass


@main.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--force", is_flag=True, help="Re-index all files")
@click.option("--no-progress", is_flag=True, help="Disable progress bar")
def index(directory, force, no_progress):
    """Index files in a directory."""
    directory_path = Path(directory).absolute()

    console.print(f"[blue]Indexing:[/blue] {directory_path}")
    console.print()

    try:
        # Ensure database exists
        db_path = get_db_path()
        if not db_path.exists():
            console.print("[yellow]Initializing database...[/yellow]")
            schema = initialize_database(db_path)
            schema.disconnect()

        # Run indexer
        indexer = Indexer(db_path=db_path)
        stats = indexer.index_directory(directory_path, show_progress=not no_progress)
        embedding_indexer = EmbeddingIndexer(
            db_path=db_path,
            vector_db_path=get_vector_db_path(),
        )
        search_indexed = embedding_indexer.index_all_pending()

        console.print()
        console.print("[green]✓ Indexing complete![/green]")
        console.print()

        # Display results table
        table = Table(title="Indexing Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta", justify="right")

        table.add_row("Files Discovered", str(stats["files_discovered"]))
        table.add_row("Files Indexed", str(stats["files_indexed"]))
        table.add_row("Files Updated", str(stats["files_updated"]))
        table.add_row("Files Skipped", str(stats["files_skipped"]))
        table.add_row("Search Indexed", str(search_indexed))
        table.add_row("Errors", str(stats["errors"]))

        console.print(table)
        console.print()

        # Save to config
        indexed_dirs = config.get("index.directories", [])
        if str(directory_path) not in indexed_dirs:
            indexed_dirs.append(str(directory_path))
            config.set("index.directories", indexed_dirs)

    except Exception as e:
        console.print(f"[red]✗ Error:[/red] {e}")
        raise click.Abort()


@main.command()
def status():
    """Show LSIEE status and statistics."""
    console.print("[bold]LSIEE Status[/bold]")
    console.print()

    db_path = get_db_path()

    if not db_path.exists():
        console.print("[yellow]⚠ Database not initialized[/yellow]")
        console.print("Run 'lsiee index <directory>' to start")
        return

    with MetadataDB(db_path) as db:
        stats = db.get_stats()

    console.print(f"[green]✓[/green] Database: {db_path}")
    console.print()

    # Display stats table
    table = Table(title="Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta", justify="right")

    table.add_row("Total Files", str(stats["total_files"]))
    table.add_row("Indexed Files", str(stats["indexed_count"]))
    table.add_row("Failed Files", str(stats["failed_count"]))
    table.add_row("Total Size", f"{stats['total_size_bytes'] / (1024**3):.2f} GB")

    console.print(table)
    console.print()

    # Show indexed directories
    indexed_dirs = config.get("index.directories", [])
    if indexed_dirs:
        console.print("[bold]Indexed Directories:[/bold]")
        for dir_path in indexed_dirs:
            console.print(f"  • {dir_path}")
    else:
        console.print("[dim]No directories indexed yet[/dim]")


@main.command()
@click.argument("query")
@click.option("--max-results", default=10, help="Maximum results")
def search(query, max_results):
    """Search files by semantic meaning."""
    console.print(f"[blue]Searching for:[/blue] {query}")
    console.print()

    searcher = SemanticSearch(
        db_path=get_db_path(),
        vector_db_path=get_vector_db_path(),
    )
    results = searcher.search(query, max_results=max_results)

    if not results:
        console.print("[yellow]No results found[/yellow]")
        return

    for i, result in enumerate(results, 1):
        console.print(f"[bold]{i}. {result['metadata']['filename']}[/bold]")
        console.print(f"   Path: {result['file_path']}")
        console.print(f"   Similarity: {result['similarity']:.2%}")
        console.print(f"   Snippet: {result['snippet']}...")
        console.print()


@main.command()
@click.argument("filepath", type=click.Path(exists=True))
@click.option("--sheet", default=None, help="Excel sheet name")
@click.option("--json-path", "json_path", default=None, help="Extract a value from a JSON path")
def inspect(filepath, sheet, json_path):
    """Inspect structured file contents."""
    filepath = Path(filepath)
    extension = filepath.suffix.lower()
    parser = StructuredDataParser()
    detector = SchemaDetector()

    console.print(f"[blue]Inspecting:[/blue] {filepath}")
    console.print()

    if extension == ".csv":
        data = parser.parse_csv(filepath)
        schema = detector.detect_csv_schema(filepath)
        if "error" in data:
            console.print(f"[red]✗ Error:[/red] {data['error']}")
            raise click.Abort()

        console.print("[green]CSV File[/green]")
        console.print(f"Rows: {data['row_count']}")
        console.print(f"Columns: {data['column_count']}")
        console.print()
        _print_schema_table(schema)
        return

    if extension in [".xlsx", ".xls"]:
        data = parser.parse_excel(filepath, sheet_name=sheet)
        if "error" in data:
            console.print(f"[red]✗ Error:[/red] {data['error']}")
            raise click.Abort()

        if sheet:
            schemas = detector.detect_excel_schema(filepath, sheet_name=sheet)
            console.print(f"[green]Excel Sheet:[/green] {sheet}")
            console.print(f"Rows: {data['row_count']}")
            console.print(f"Columns: {data['column_count']}")
            console.print()
            _print_schema_table(schemas.get(sheet, []), title=f"Schema: {sheet}")
            return

        console.print("[green]Excel File[/green]")
        console.print(f"Sheets: {data['sheet_count']}")
        console.print()
        table = Table(title="Sheets")
        table.add_column("Sheet", style="cyan")
        table.add_column("Rows", style="magenta", justify="right")
        table.add_column("Columns", style="yellow", justify="right")
        for sheet_name, info in data["sheets"].items():
            table.add_row(sheet_name, str(info["row_count"]), str(info["column_count"]))
        console.print(table)
        return

    if extension == ".json":
        data = parser.parse_json(filepath)
        if "error" in data:
            console.print(f"[red]✗ Error:[/red] {data['error']}")
            raise click.Abort()

        console.print("[green]JSON File[/green]")
        console.print(f"Type: {data['type']}")
        console.print()

        if json_path:
            extracted = parser.extract_json_path(filepath, json_path)
            console.print(f"[bold]JSON Path:[/bold] {json_path}")
            console.print(JSON.from_data(extracted))
            return

        console.print("[bold]Structure:[/bold]")
        console.print(JSON.from_data(data["structure"]))
        console.print()
        console.print("[bold]Sample:[/bold]")
        console.print(data["sample"])
        return

    console.print(f"[yellow]Unsupported file type: {extension}[/yellow]")


def _print_schema_table(schema, title="Schema"):
    """Render schema information as a Rich table."""
    table = Table(title=title)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Nulls", style="red", justify="right")
    table.add_column("Unique", style="green", justify="right")

    for column in schema:
        table.add_row(
            str(column["column_name"]),
            str(column["column_type"]),
            str(column["null_count"]),
            str(column["unique_count"]),
        )

    console.print(table)


@main.command()
@click.argument("filepath", type=click.Path(exists=True))
@click.argument("query")
@click.option("--export", type=click.Path(), help="Export results to a file")
def query(filepath, query, export):
    """Query structured data files using natural language."""
    executor = QueryExecutor()
    formatter = ResultFormatter()
    filepath = Path(filepath)

    console.print(f"[blue]Querying:[/blue] {filepath}")
    console.print(f"[blue]Query:[/blue] {query}")
    console.print()

    result = executor.execute_query_safe(filepath, query)
    if "error" in result:
        console.print(f"[red]✗ Error:[/red] {result['error']}")
        raise click.Abort()

    console.print("[green]Results:[/green]")
    payload = result["result"]

    if isinstance(payload, (int, float)):
        console.print(payload)
    elif isinstance(payload, dict):
        console.print(JSON.from_data(payload))
    else:
        console.print(formatter.format_table(payload))

    if export:
        export_path = Path(export)
        export_format = "json" if export_path.suffix.lower() == ".json" else "csv"
        formatter.export_to_file(payload, export_path, format=export_format)
        console.print()
        console.print(f"[green]Exported to:[/green] {export_path}")


@main.command()
@click.option("--start", "start_monitoring", is_flag=True, help="Start background monitoring")
@click.option("--stop", "stop_monitoring", is_flag=True, help="Stop background monitoring")
@click.option("--status", "show_status", is_flag=True, help="Show daemon status")
@click.option("--top-cpu", is_flag=True, help="Show top CPU-consuming processes")
@click.option("--top-memory", is_flag=True, help="Show top memory-consuming processes")
@click.option("--system", "show_system", is_flag=True, help="Show system-wide metrics")
@click.option("--process-name", default=None, help="Filter live processes by name")
@click.option("--history-pid", type=int, default=None, help="Show recent stored history for a PID")
@click.option("--timeline", default=None, help="Show stored CPU timeline for a process name")
@click.option("--detect-anomalies", is_flag=True, help="Detect anomalies using recent history")
@click.option("--alert-history", is_flag=True, help="Show recent logged anomaly alerts")
@click.option("--hours", default=24, show_default=True, help="History window in hours")
@click.option("--limit", default=10, show_default=True, help="Maximum rows to display")
@click.option("--interval", type=float, default=None, help="Monitoring interval in seconds")
@click.option(
    "--iterations",
    type=int,
    default=None,
    help="Run a bounded number of monitoring iterations in the foreground",
)
def monitor(
    start_monitoring,
    stop_monitoring,
    show_status,
    top_cpu,
    top_memory,
    show_system,
    process_name,
    history_pid,
    timeline,
    detect_anomalies,
    alert_history,
    hours,
    limit,
    interval,
    iterations,
):
    """Monitor system processes and resource usage."""
    db_path = get_db_path()
    process_monitor = ProcessMonitor()
    history = ProcessHistory(db_path)
    metrics = SystemMetrics()
    alert_manager = AlertManager(db_path=db_path)

    if interval is not None:
        config.set("monitoring.interval_seconds", interval)

    if start_monitoring:
        if iterations is not None:
            daemon = MonitoringDaemon(db_path=db_path, interval=interval)
            daemon.start(blocking=True, iterations=iterations)
            console.print(
                f"[green]✓ Collected {iterations} monitoring iteration(s) into {db_path}[/green]"
            )
            return

        pid = spawn_background_daemon(db_path=db_path, interval=interval)
        config.set("monitoring.enabled", True)
        console.print(f"[green]✓ Monitoring daemon started[/green] (PID: {pid})")
        return

    if stop_monitoring:
        stopped = stop_background_daemon(db_path=db_path)
        config.set("monitoring.enabled", False)
        if stopped:
            console.print("[green]✓ Monitoring daemon stopped[/green]")
        else:
            console.print("[yellow]No monitoring daemon was running[/yellow]")
        return

    if show_status:
        status_info = get_daemon_status(db_path=db_path)
        table = Table(title="Monitoring Status")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        table.add_row("State", "Running" if status_info["running"] else "Stopped")
        table.add_row("PID", str(status_info["pid"] or "-"))
        table.add_row("DB Path", status_info["db_path"])
        table.add_row("PID File", status_info["pid_path"])
        table.add_row("Interval (s)", str(status_info["interval_seconds"]))
        console.print(table)
        return

    if top_cpu:
        _print_process_table(process_monitor.get_top_cpu(n=limit), title="Top CPU Processes")
        return

    if top_memory:
        _print_process_table(
            process_monitor.get_top_memory(n=limit),
            title="Top Memory Processes",
        )
        return

    if show_system:
        _print_system_metrics(metrics.get_all_metrics())
        return

    if process_name:
        matches = process_monitor.get_process_by_name(process_name)[:limit]
        if not matches:
            console.print(f"[yellow]No running processes matched '{process_name}'[/yellow]")
            return
        _print_process_table(matches, title=f"Processes Matching '{process_name}'")
        return

    if history_pid is not None:
        end_time = datetime.now().timestamp()
        start_time = (datetime.now() - timedelta(hours=hours)).timestamp()
        rows = history.get_process_history(history_pid, start_time, end_time)
        if not rows:
            console.print(f"[yellow]No stored history found for PID {history_pid}[/yellow]")
            return
        _print_history_table(rows[:limit], title=f"History for PID {history_pid}")
        return

    if timeline:
        rows = history.get_cpu_timeline(timeline, hours=hours)
        if not rows:
            console.print(f"[yellow]No stored CPU timeline found for '{timeline}'[/yellow]")
            return
        _print_timeline_table(rows[:limit], title=f"CPU Timeline: {timeline}")
        return

    if detect_anomalies:
        min_training_samples = int(config.get("anomaly_detection.min_training_samples", 25))
        training_rows = history.get_recent_history(
            hours=hours,
            limit=max(limit * 20, min_training_samples),
        )
        if len(training_rows) < min_training_samples:
            console.print(
                "[yellow]Not enough stored history to train anomaly detector. "
                f"Need {min_training_samples} rows, found {len(training_rows)}.[/yellow]"
            )
            return

        detector = AnomalyDetector(
            contamination=float(config.get("anomaly_detection.contamination", 0.1)),
            min_samples=min_training_samples,
        )
        detector.fit(training_rows)

        live_snapshot = process_monitor.capture_snapshot()
        anomalies = []
        alerts = []

        for proc in live_snapshot:
            prediction = detector.predict(proc)
            proc_alerts = alert_manager.check_thresholds(proc, prediction=prediction)
            alerts.extend(proc_alerts)
            if prediction["is_anomaly"]:
                anomalies.append(
                    {
                        **prediction,
                        "cpu_percent": proc["cpu_percent"],
                        "memory_mb": proc["memory_mb"],
                        "memory_percent": proc["memory_percent"],
                        "num_threads": proc["num_threads"],
                    }
                )

        if alerts:
            alert_manager.log_alerts(alerts)

        if not anomalies:
            console.print("[green]No anomalies detected in the current snapshot[/green]")
        else:
            _print_anomaly_table(anomalies[:limit], title="Detected Anomalies")
            if alerts:
                console.print()
                console.print(f"[yellow]Logged {len(alerts)} alert(s) to the events store[/yellow]")
        return

    if alert_history:
        rows = alert_manager.get_recent_alerts(hours=hours, limit=limit)
        if not rows:
            console.print("[yellow]No anomaly alerts found in the requested time window[/yellow]")
            return
        _print_alert_table(rows, title="Anomaly Alerts")
        return

    console.print("[bold]Live Monitoring Overview[/bold]")
    console.print()
    _print_system_metrics(metrics.get_all_metrics())
    console.print()
    _print_process_table(process_monitor.get_top_cpu(n=limit), title="Top CPU Processes")


@main.command()
@click.argument("issue")
def explain(issue):
    """Explain system issues (Coming in Week 11!)."""
    console.print(f"[yellow]🚧 Explain feature coming in Week 11![/yellow]")
    console.print(f"Issue: {issue}")


def _print_process_table(processes, title):
    """Render a process list as a Rich table."""
    table = Table(title=title)
    table.add_column("PID", style="cyan", justify="right")
    table.add_column("Name", style="yellow")
    table.add_column("CPU %", style="magenta", justify="right")
    table.add_column("Memory MB", style="green", justify="right")
    table.add_column("Status", style="blue")
    table.add_column("Threads", style="white", justify="right")

    for proc in processes:
        table.add_row(
            str(proc["pid"]),
            str(proc["name"]),
            f"{proc['cpu_percent']:.2f}",
            f"{proc['memory_mb']:.2f}",
            str(proc["status"]),
            str(proc["num_threads"]),
        )

    console.print(table)


def _print_system_metrics(metrics):
    """Render system metrics tables."""
    cpu = Table(title="CPU")
    cpu.add_column("Metric", style="cyan")
    cpu.add_column("Value", style="magenta")
    cpu.add_row("Percent", f"{metrics['cpu']['percent']:.2f}")
    cpu.add_row("Logical CPUs", str(metrics["cpu"]["count_logical"]))
    cpu.add_row("Physical CPUs", str(metrics["cpu"]["count_physical"]))
    cpu.add_row("Per CPU", ", ".join(f"{value:.1f}" for value in metrics["cpu"]["per_cpu"]))
    console.print(cpu)

    memory = Table(title="Memory")
    memory.add_column("Metric", style="cyan")
    memory.add_column("Value", style="magenta")
    memory.add_row("Used GB", f"{metrics['memory']['used_gb']:.2f}")
    memory.add_row("Available GB", f"{metrics['memory']['available_gb']:.2f}")
    memory.add_row("Total GB", f"{metrics['memory']['total_gb']:.2f}")
    memory.add_row("Percent", f"{metrics['memory']['percent']:.2f}")
    memory.add_row("Swap Used GB", f"{metrics['memory']['swap_used_gb']:.2f}")
    memory.add_row("Swap Percent", f"{metrics['memory']['swap_percent']:.2f}")
    console.print(memory)

    network = Table(title="Network")
    network.add_column("Metric", style="cyan")
    network.add_column("Value", style="magenta")
    network.add_row("Bytes Sent", str(metrics["network"]["bytes_sent"]))
    network.add_row("Bytes Received", str(metrics["network"]["bytes_recv"]))
    network.add_row("Packets Sent", str(metrics["network"]["packets_sent"]))
    network.add_row("Packets Received", str(metrics["network"]["packets_recv"]))
    console.print(network)

    disk = Table(title="Disk Partitions")
    disk.add_column("Mount", style="cyan")
    disk.add_column("Used %", style="magenta", justify="right")
    disk.add_column("Used GB", style="yellow", justify="right")
    disk.add_column("Free GB", style="green", justify="right")
    for partition in metrics["disk"]["partitions"]:
        disk.add_row(
            partition["mountpoint"],
            f"{partition['percent']:.2f}",
            f"{partition['used_gb']:.2f}",
            f"{partition['free_gb']:.2f}",
        )
    console.print(disk)


def _print_history_table(rows, title):
    """Render stored process history rows."""
    table = Table(title=title)
    table.add_column("Timestamp", style="cyan")
    table.add_column("PID", style="yellow", justify="right")
    table.add_column("Name", style="magenta")
    table.add_column("CPU %", style="green", justify="right")
    table.add_column("Memory MB", style="blue", justify="right")
    table.add_column("Status", style="white")

    for row in rows:
        table.add_row(
            datetime.fromtimestamp(row["timestamp"]).isoformat(timespec="seconds"),
            str(row["pid"]),
            str(row["name"]),
            f"{row['cpu_percent']:.2f}",
            f"{row['memory_mb']:.2f}",
            str(row["status"]),
        )

    console.print(table)


def _print_timeline_table(rows, title):
    """Render CPU timeline points."""
    table = Table(title=title)
    table.add_column("Timestamp", style="cyan")
    table.add_column("CPU %", style="magenta", justify="right")

    for timestamp, cpu_percent in rows:
        table.add_row(
            datetime.fromtimestamp(timestamp).isoformat(timespec="seconds"),
            f"{cpu_percent:.2f}",
        )

    console.print(table)


def _print_anomaly_table(rows, title):
    """Render anomaly detection results."""
    table = Table(title=title)
    table.add_column("PID", style="cyan", justify="right")
    table.add_column("Process", style="yellow")
    table.add_column("Score", style="magenta", justify="right")
    table.add_column("CPU %", style="green", justify="right")
    table.add_column("Memory MB", style="blue", justify="right")
    table.add_column("Threads", style="white", justify="right")

    for row in rows:
        table.add_row(
            str(row["pid"]),
            str(row["process_name"]),
            f"{row['anomaly_score']:.4f}",
            f"{row['cpu_percent']:.2f}",
            f"{row['memory_mb']:.2f}",
            str(row["num_threads"]),
        )

    console.print(table)


def _print_alert_table(rows, title):
    """Render recent alert history rows."""
    table = Table(title=title)
    table.add_column("Timestamp", style="cyan")
    table.add_column("Event", style="yellow")
    table.add_column("Severity", style="magenta")
    table.add_column("Process", style="green")
    table.add_column("Message", style="white")

    for row in rows:
        table.add_row(
            datetime.fromtimestamp(row["timestamp"]).isoformat(timespec="seconds"),
            str(row["event_type"]),
            str(row["severity"]),
            str(row.get("process_name") or "-"),
            str(row.get("message") or "-"),
        )

    console.print(table)


if __name__ == "__main__":
    main()
