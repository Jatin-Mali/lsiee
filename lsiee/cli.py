"""LSIEE Command Line Interface."""

import click
from pathlib import Path
import logging
from rich.console import Console
from rich.table import Table

from lsiee.config import config, get_db_path, get_vector_db_path
from lsiee.file_intelligence.indexing.embedding_indexer import EmbeddingIndexer
from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.file_intelligence.search.semantic_search import SemanticSearch
from lsiee.storage.metadata_db import MetadataDB
from lsiee.storage.schemas import initialize_database

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
def inspect(filepath):
    """Inspect file structure (Coming in Week 3!)."""
    console.print(f"[yellow]🚧 Inspect feature coming in Week 3![/yellow]")
    console.print(f"File: {filepath}")


@main.command()
def monitor():
    """Monitor system processes (Coming in Week 5!)."""
    console.print("[yellow]🚧 Monitor feature coming in Week 5![/yellow]")


@main.command()
@click.argument("issue")
def explain(issue):
    """Explain system issues (Coming in Week 11!)."""
    console.print(f"[yellow]🚧 Explain feature coming in Week 11![/yellow]")
    console.print(f"Issue: {issue}")


if __name__ == "__main__":
    main()
