"""LSIEE Command Line Interface."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import click
from rich.console import Console
from rich.json import JSON
from rich.table import Table

from lsiee import __version__
from lsiee.config import config, get_data_dir, get_db_path, get_vector_db_path
from lsiee.file_intelligence.data_extraction.parsers import StructuredDataParser
from lsiee.file_intelligence.data_extraction.query_executor import QueryExecutor
from lsiee.file_intelligence.data_extraction.result_formatter import ResultFormatter
from lsiee.file_intelligence.data_extraction.schema_detector import SchemaDetector
from lsiee.file_intelligence.indexing.embedding_indexer import EmbeddingIndexer
from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.file_intelligence.search.semantic_search import SemanticSearch
from lsiee.security import (
    PathSecurityError,
    cleanup_lsiee_data,
    display_path,
    ensure_safe_directory,
    ensure_safe_file,
    ensure_safe_output_path,
    export_lsiee_data,
    purge_lsiee_data,
    safe_rich_text,
    sanitize_terminal_data,
    sanitize_terminal_text,
    validate_generic_text,
    validate_json_path,
    validate_positive_float,
    validate_positive_int,
    validate_query_text,
)
from lsiee.security.verification import verify_lsiee_runtime
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
from lsiee.temporal_intelligence.explanation import RootCauseAnalyzer
from lsiee.temporal_intelligence.explanation.root_cause import parse_issue_timestamp
from lsiee.utils.logging_utils import setup_logging

console = Console()


def _safe(value, *, max_length: int = 4096, single_line: bool = True) -> str:
    """Escape terminal text for Rich output."""
    return safe_rich_text(value, max_length=max_length, single_line=single_line)


def _safe_path(path) -> str:
    """Render a path through the redaction and terminal-safety pipeline."""
    return _safe(display_path(path))


def _print_json(value) -> None:
    """Render JSON after recursively stripping terminal control sequences."""
    console.print(JSON.from_data(sanitize_terminal_data(value)))


@click.group()
@click.version_option(version=__version__)
def main():
    """LSIEE - Local System Intelligence & Execution Engine

    A local-first system intelligence platform for understanding
    and interacting with your operating system.
    """
    setup_logging(level=getattr(logging, str(config.get("logging.level", "WARNING")).upper()))


@main.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--force", is_flag=True, help="Re-index all files")
@click.option("--no-progress", is_flag=True, help="Disable progress bar")
def index(directory, force, no_progress):
    """Index files in a directory."""
    try:
        directory_path = ensure_safe_directory(Path(directory))
    except PathSecurityError:
        console.print("[red]✗ Error:[/red] Access denied or invalid directory")
        raise click.Abort()

    console.print(f"[blue]Indexing:[/blue] {_safe_path(directory_path)}")
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
        stats = indexer.index_directory(directory_path, show_progress=not no_progress, force=force)
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
        table.add_row("Files Unchanged", str(stats.get("files_unchanged", 0)))
        table.add_row("Files Skipped", str(stats["files_skipped"]))
        table.add_row("Permission Denied", str(stats.get("permission_denied", 0)))
        table.add_row("Too Large", str(stats.get("too_large", 0)))
        table.add_row("Unsafe Paths", str(stats.get("unsafe_paths", 0)))
        table.add_row("Search Indexed", str(search_indexed))
        table.add_row("Errors", str(stats["errors"]))

        console.print(table)
        console.print()

        # Save to config
        indexed_dirs = config.get("index.directories", [])
        if str(directory_path) not in indexed_dirs:
            indexed_dirs.append(str(directory_path))
            config.set("index.directories", indexed_dirs)

    except Exception:
        console.print("[red]✗ Error:[/red] Indexing failed")
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

    console.print(f"[green]✓[/green] Database: {_safe_path(db_path)}")
    console.print(f"[green]✓[/green] Data Directory: {_safe_path(get_data_dir())}")
    console.print("[dim]All LSIEE data remains local to this computer.[/dim]")
    console.print()

    # Display stats table
    table = Table(title="Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta", justify="right")

    table.add_row("Total Files", str(stats["total_files"]))
    table.add_row("Indexed Files", str(stats["indexed_count"]))
    table.add_row("Pending Files", str(stats["pending_count"]))
    table.add_row("Skipped Files", str(stats["skipped_count"]))
    table.add_row("Failed Files", str(stats["failed_count"]))
    table.add_row("Total Size", f"{stats['total_size_bytes'] / (1024**3):.2f} GB")

    console.print(table)
    console.print()

    vector_stats = SemanticSearch(
        db_path=db_path,
        vector_db_path=get_vector_db_path(),
    ).vector_db.get_diagnostics()
    search_table = Table(title="Search Index")
    search_table.add_column("Metric", style="cyan")
    search_table.add_column("Value", style="magenta", justify="right")
    search_table.add_row("Stored Vectors", str(vector_stats["vector_count"]))
    search_table.add_row("Consistent", "Yes" if vector_stats["is_consistent"] else "No")
    search_table.add_row("Vectors File", _safe_path(vector_stats["vectors_file"]))
    console.print(search_table)
    console.print()

    # Show indexed directories
    indexed_dirs = config.get("index.directories", [])
    if indexed_dirs:
        console.print("[bold]Indexed Directories:[/bold]")
        for dir_path in indexed_dirs:
            console.print(f"  • {_safe_path(dir_path)}")
    else:
        console.print("[dim]No directories indexed yet[/dim]")


@main.command()
def verify():
    """Verify LSIEE local state, database integrity, and search consistency."""
    report = verify_lsiee_runtime(
        db_path=get_db_path(),
        vector_db_path=get_vector_db_path(),
        config_file=config.config_file,
        log_dir=get_data_dir() / "logs",
    )

    table = Table(title="Verification Report")
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Details", style="white")

    for check in report["checks"]:
        status_text = "[green]PASS[/green]" if check["ok"] else "[red]FAIL[/red]"
        table.add_row(
            sanitize_terminal_text(check["name"]),
            status_text,
            sanitize_terminal_text(check["details"], max_length=256, single_line=False),
        )

    console.print(table)
    console.print()

    if report["ok"]:
        console.print(f"[green]✓ Verification passed[/green] " f"({report['passed_count']} checks)")
        return

    console.print(
        f"[red]✗ Verification failed[/red] " f"({report['failed_count']} failing check(s))"
    )
    raise click.Abort()


@main.command()
@click.argument("query")
@click.option("--max-results", default=10, help="Maximum results")
def search(query, max_results):
    """Search files by semantic meaning."""
    try:
        query = validate_query_text(
            query,
            max_length=int(config.get("security.max_query_length", 500)),
            max_conditions=int(config.get("security.max_query_conditions", 3)),
        )
        max_results = validate_positive_int(max_results, name="max_results", maximum=1000)
    except ValueError as exc:
        console.print(f"[red]✗ Error:[/red] {_safe(exc)}")
        raise click.Abort()

    console.print(f"[blue]Searching for:[/blue] {_safe(query)}")
    console.print()

    searcher = SemanticSearch(
        db_path=get_db_path(),
        vector_db_path=get_vector_db_path(),
    )
    results = searcher.search(query, max_results=max_results)

    if not results:
        with MetadataDB(get_db_path()) as db:
            stats = db.get_stats()
        diagnostics = searcher.vector_db.get_diagnostics()

        if stats["indexed_count"] == 0 and stats["pending_count"] > 0:
            console.print(
                "[yellow]Search index is incomplete. "
                "Re-run indexing with --force to rebuild it.[/yellow]"
            )
        elif stats["indexed_count"] == 0 and stats["skipped_count"] > 0:
            console.print(
                "[yellow]No searchable files were indexed. "
                "Current files were skipped as unsupported or non-text.[/yellow]"
            )
        elif (
            not diagnostics["is_consistent"]
            or diagnostics["vector_count"] != stats["indexed_count"]
        ):
            console.print(
                "[yellow]Search index integrity check failed. "
                "Re-run indexing with --force to repair it.[/yellow]"
            )
        else:
            console.print("[yellow]No matches found. Try different search terms.[/yellow]")
        return

    for i, result in enumerate(results, 1):
        console.print(f"[bold]{i}. {_safe(result['metadata']['filename'])}[/bold]")
        console.print(f"   Path: {_safe_path(result['file_path'])}")
        console.print(f"   Similarity: {result['similarity']:.2%}")
        console.print(f"   Snippet: {_safe(result['snippet'], max_length=240)}...")
        console.print()


@main.command()
@click.argument("filepath", type=click.Path(exists=True))
@click.option("--sheet", default=None, help="Excel sheet name")
@click.option("--json-path", "json_path", default=None, help="Extract a value from a JSON path")
def inspect(filepath, sheet, json_path):
    """Inspect structured file contents."""
    try:
        filepath = ensure_safe_file(
            Path(filepath),
            max_size_bytes=int(config.get("security.max_parse_file_size_mb", 100) * 1024 * 1024),
        )
        json_path = validate_json_path(json_path)
    except (PathSecurityError, ValueError):
        console.print("[red]✗ Error:[/red] Access denied or invalid file")
        raise click.Abort()
    extension = filepath.suffix.lower()
    parser = StructuredDataParser()
    detector = SchemaDetector()

    console.print(f"[blue]Inspecting:[/blue] {_safe_path(filepath)}")
    console.print()

    if extension == ".csv":
        data = parser.parse_csv(filepath)
        schema = detector.detect_csv_schema(filepath)
        if "error" in data:
            console.print(f"[red]✗ Error:[/red] {_safe(data['error'])}")
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
            console.print(f"[red]✗ Error:[/red] {_safe(data['error'])}")
            raise click.Abort()

        if sheet:
            schemas = detector.detect_excel_schema(filepath, sheet_name=sheet)
            console.print(f"[green]Excel Sheet:[/green] {_safe(sheet)}")
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
            table.add_row(
                sanitize_terminal_text(sheet_name),
                str(info["row_count"]),
                str(info["column_count"]),
            )
        console.print(table)
        return

    if extension == ".json":
        data = parser.parse_json(filepath)
        if "error" in data:
            console.print(f"[red]✗ Error:[/red] {_safe(data['error'])}")
            raise click.Abort()

        console.print("[green]JSON File[/green]")
        console.print(f"Type: {data['type']}")
        console.print()

        if json_path:
            try:
                extracted = parser.extract_json_path(filepath, json_path)
            except Exception:
                console.print("[red]✗ Error:[/red] Invalid JSON path")
                raise click.Abort()
            console.print(f"[bold]JSON Path:[/bold] {_safe(json_path)}")
            _print_json(extracted)
            return

        console.print("[bold]Structure:[/bold]")
        _print_json(data["structure"])
        console.print()
        console.print("[bold]Sample:[/bold]")
        _print_json(data["sample"])
        return

    console.print(f"[yellow]Unsupported file type: {_safe(extension)}[/yellow]")


def _print_schema_table(schema, title="Schema"):
    """Render schema information as a Rich table."""
    table = Table(title=title)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Nulls", style="red", justify="right")
    table.add_column("Unique", style="green", justify="right")

    for column in schema:
        table.add_row(
            sanitize_terminal_text(column["column_name"]),
            sanitize_terminal_text(column["column_type"]),
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
    try:
        filepath = ensure_safe_file(
            Path(filepath),
            max_size_bytes=int(config.get("security.max_parse_file_size_mb", 100) * 1024 * 1024),
        )
        query = validate_query_text(
            query,
            max_length=int(config.get("security.max_query_length", 500)),
            max_conditions=int(config.get("security.max_query_conditions", 3)),
        )
        if export:
            ensure_safe_output_path(Path(export))
    except (PathSecurityError, ValueError) as exc:
        console.print(f"[red]✗ Error:[/red] {_safe(exc)}")
        raise click.Abort()

    console.print(f"[blue]Querying:[/blue] {_safe_path(filepath)}")
    console.print(f"[blue]Query:[/blue] {_safe(query)}")
    console.print()

    result = executor.execute_query_safe(filepath, query)
    if "error" in result:
        console.print(f"[red]✗ Error:[/red] {_safe(result['error'])}")
        raise click.Abort()

    console.print("[green]Results:[/green]")
    payload = result["result"]

    if isinstance(payload, (int, float)):
        console.print(payload)
    elif isinstance(payload, dict):
        _print_json(payload)
    else:
        console.print(formatter.format_table(payload))

    if export:
        export_path = ensure_safe_output_path(Path(export))
        export_format = "json" if export_path.suffix.lower() == ".json" else "csv"
        formatter.export_to_file(payload, export_path, format=export_format)
        console.print()
        console.print(f"[green]Exported to:[/green] {_safe_path(export_path)}")


@main.command(name="export")
@click.option(
    "--format",
    "export_format",
    type=click.Choice(["json", "csv"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Export JSON or a ZIP bundle of CSV files.",
)
@click.option("--output", type=click.Path(), required=True, help="Destination export file")
def export_data(export_format, output):
    """Export local LSIEE data for review or portability."""
    try:
        summary = export_lsiee_data(
            db_path=get_db_path(),
            vector_db_path=get_vector_db_path(),
            config_file=config.config_file,
            output_path=Path(output),
            format=export_format.lower(),
        )
    except (PathSecurityError, ValueError) as exc:
        console.print(f"[red]✗ Error:[/red] {_safe(exc)}")
        raise click.Abort()

    console.print(f"[green]✓ Export complete[/green] {_safe_path(summary['output_path'])}")
    table = Table(title="Export Summary")
    table.add_column("Dataset", style="cyan")
    table.add_column("Rows", style="magenta", justify="right")
    for key, value in summary["counts"].items():
        table.add_row(sanitize_terminal_text(key), str(value))
    console.print(table)


@main.command()
@click.option(
    "--type",
    "cleanup_type",
    type=click.Choice(["process-snapshots", "events", "all"], case_sensitive=False),
    default="all",
    show_default=True,
)
@click.option("--older-than", "older_than", type=int, default=None, help="Retention window in days")
@click.option("--dry-run", is_flag=True, help="Show what would be deleted")
@click.option("--yes", is_flag=True, help="Skip the interactive confirmation prompt")
def cleanup(cleanup_type, older_than, dry_run, yes):
    """Delete aged local monitoring or event data."""
    try:
        older_than_days = (
            validate_positive_int(older_than, name="older_than", maximum=3650)
            if older_than is not None
            else None
        )
    except ValueError as exc:
        console.print(f"[red]✗ Error:[/red] {_safe(exc)}")
        raise click.Abort()

    preview = cleanup_lsiee_data(
        db_path=get_db_path(),
        data_type=cleanup_type,
        older_than_days=older_than_days,
        dry_run=True,
    )
    matched_rows = sum(plan["matched_rows"] for plan in preview["plans"])

    if matched_rows == 0:
        console.print("[yellow]No matching data met the cleanup criteria.[/yellow]")
        return

    table = Table(title="Cleanup Preview" if dry_run else "Cleanup Targets")
    table.add_column("Table", style="cyan")
    table.add_column("Rows", style="magenta", justify="right")
    table.add_column("Oldest", style="yellow")
    table.add_column("Newest", style="green")

    for plan in preview["plans"]:
        oldest = (
            datetime.fromtimestamp(plan["oldest_timestamp"]).isoformat(timespec="seconds")
            if plan["oldest_timestamp"]
            else "-"
        )
        newest = (
            datetime.fromtimestamp(plan["newest_timestamp"]).isoformat(timespec="seconds")
            if plan["newest_timestamp"]
            else "-"
        )
        table.add_row(
            sanitize_terminal_text(plan["table"]),
            str(plan["matched_rows"]),
            sanitize_terminal_text(oldest),
            sanitize_terminal_text(newest),
        )
    console.print(table)

    if dry_run:
        return

    if not yes:
        click.confirm(f"Delete {matched_rows} row(s) from local LSIEE data?", abort=True)

    summary = cleanup_lsiee_data(
        db_path=get_db_path(),
        data_type=cleanup_type,
        older_than_days=older_than_days,
        dry_run=False,
    )
    console.print(f"[green]✓ Cleanup complete[/green] Deleted {summary['deleted_rows']} row(s)")


@main.command(name="delete-all-data")
@click.option("--confirm", "confirm_token", default="", help="Type DELETE to confirm")
def delete_all_data(confirm_token):
    """Permanently remove LSIEE local databases, vectors, config, and logs."""
    if confirm_token != "DELETE":
        console.print("[red]✗ Error:[/red] This operation requires `--confirm DELETE`.")
        raise click.Abort()

    try:
        summary = purge_lsiee_data(
            db_path=get_db_path(),
            vector_db_path=get_vector_db_path(),
            config_file=config.config_file,
            log_dir=get_data_dir() / "logs",
        )
    except (PathSecurityError, ValueError) as exc:
        console.print(f"[red]✗ Error:[/red] {_safe(exc)}")
        raise click.Abort()

    config._config = config._default_config()
    console.print(f"[green]✓ Deleted {len(summary['removed'])} LSIEE artifact(s)[/green]")
    for path in summary["removed"]:
        console.print(f"  • {_safe_path(path)}")


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
    try:
        hours = validate_positive_int(hours, name="hours", maximum=24 * 365)
        limit = validate_positive_int(limit, name="limit", maximum=1000)
        if iterations is not None:
            iterations = validate_positive_int(iterations, name="iterations", maximum=100000)
        if interval is not None:
            interval = validate_positive_float(
                interval,
                name="interval",
                minimum=0.01,
                maximum=3600.0,
            )
        if process_name:
            process_name = validate_generic_text(process_name, name="process_name", max_length=128)
        if timeline:
            timeline = validate_generic_text(
                timeline,
                name="timeline",
                max_length=128,
                reject_shell_metacharacters=True,
            )
    except ValueError as exc:
        console.print(f"[red]✗ Error:[/red] {_safe(exc)}")
        raise click.Abort()

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
                "[green]✓ Collected "
                f"{iterations} monitoring iteration(s) into {_safe_path(db_path)}[/green]"
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
        table.add_row("DB Path", _safe_path(status_info["db_path"]))
        table.add_row("PID File", _safe_path(status_info["pid_path"]))
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
            console.print(f"[yellow]No running processes matched '{_safe(process_name)}'[/yellow]")
            return
        _print_process_table(
            matches, title=f"Processes Matching '{sanitize_terminal_text(process_name)}'"
        )
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
            console.print(f"[yellow]No stored CPU timeline found for '{_safe(timeline)}'[/yellow]")
            return
        _print_timeline_table(
            rows[:limit], title=f"CPU Timeline: {sanitize_terminal_text(timeline)}"
        )
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
@click.option("--time", "issue_time", default=None, help="Incident timestamp (ISO-8601 or epoch)")
def explain(issue, issue_time):
    """Explain system issues and provide recommendations."""
    analyzer = RootCauseAnalyzer(db_path=get_db_path())

    try:
        issue = validate_generic_text(issue, name="issue", max_length=256)
        timestamp = parse_issue_timestamp(issue_time)
        explanation = analyzer.explain_issue(issue, timestamp)
    except ValueError as exc:
        console.print(f"[red]Error:[/red] {_safe(exc)}")
        raise click.Abort() from exc

    console.print(f"[bold]Issue:[/bold] {_safe(explanation['issue'])}")
    console.print(
        "[bold]Time:[/bold] "
        f"{datetime.fromtimestamp(explanation['timestamp']).isoformat(timespec='seconds')}"
    )
    console.print()

    console.print("[bold]Root Causes:[/bold]")
    for cause in explanation["root_causes"]:
        console.print(f"  - {_safe(cause)}")
    console.print()

    console.print("[bold]Evidence:[/bold]")
    for evidence in explanation["evidence"]:
        if evidence["type"] == "process_metrics":
            count = len(evidence.get("processes", []))
        elif evidence["type"] == "events":
            count = len(evidence.get("events", []))
        elif evidence["type"] == "correlations":
            count = len(evidence.get("correlations", []))
        else:
            count = len(evidence.get("occurrences", []))
        console.print(f"  - {_safe(evidence['type'])}: {count} item(s)")
    console.print()

    console.print("[bold]Recommendations:[/bold]")
    for recommendation in explanation["recommendations"]:
        console.print(f"  - {_safe(recommendation)}")
    console.print()
    console.print(f"[dim]{_safe(explanation['disclaimer'])}[/dim]")


def _print_process_table(processes, title):
    """Render a process list as a Rich table."""
    table = Table(title=sanitize_terminal_text(title))
    table.add_column("PID", style="cyan", justify="right")
    table.add_column("Name", style="yellow")
    table.add_column("CPU %", style="magenta", justify="right")
    table.add_column("Memory MB", style="green", justify="right")
    table.add_column("Status", style="blue")
    table.add_column("Threads", style="white", justify="right")

    for proc in processes:
        table.add_row(
            str(proc["pid"]),
            sanitize_terminal_text(proc["name"]),
            f"{proc['cpu_percent']:.2f}",
            f"{proc['memory_mb']:.2f}",
            sanitize_terminal_text(proc["status"]),
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
            sanitize_terminal_text(partition["mountpoint"]),
            f"{partition['percent']:.2f}",
            f"{partition['used_gb']:.2f}",
            f"{partition['free_gb']:.2f}",
        )
    console.print(disk)


def _print_history_table(rows, title):
    """Render stored process history rows."""
    table = Table(title=sanitize_terminal_text(title))
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
            sanitize_terminal_text(row["name"]),
            f"{row['cpu_percent']:.2f}",
            f"{row['memory_mb']:.2f}",
            sanitize_terminal_text(row["status"]),
        )

    console.print(table)


def _print_timeline_table(rows, title):
    """Render CPU timeline points."""
    table = Table(title=sanitize_terminal_text(title))
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
    table = Table(title=sanitize_terminal_text(title))
    table.add_column("PID", style="cyan", justify="right")
    table.add_column("Process", style="yellow")
    table.add_column("Score", style="magenta", justify="right")
    table.add_column("CPU %", style="green", justify="right")
    table.add_column("Memory MB", style="blue", justify="right")
    table.add_column("Threads", style="white", justify="right")

    for row in rows:
        table.add_row(
            str(row["pid"]),
            sanitize_terminal_text(row["process_name"]),
            f"{row['anomaly_score']:.4f}",
            f"{row['cpu_percent']:.2f}",
            f"{row['memory_mb']:.2f}",
            str(row["num_threads"]),
        )

    console.print(table)


def _print_alert_table(rows, title):
    """Render recent alert history rows."""
    table = Table(title=sanitize_terminal_text(title))
    table.add_column("Timestamp", style="cyan")
    table.add_column("Event", style="yellow")
    table.add_column("Severity", style="magenta")
    table.add_column("Process", style="green")
    table.add_column("Message", style="white")

    for row in rows:
        table.add_row(
            datetime.fromtimestamp(row["timestamp"]).isoformat(timespec="seconds"),
            sanitize_terminal_text(row["event_type"]),
            sanitize_terminal_text(row["severity"]),
            sanitize_terminal_text(row.get("process_name") or "-"),
            sanitize_terminal_text(row.get("message") or "-", max_length=256),
        )

    console.print(table)


if __name__ == "__main__":
    main()
