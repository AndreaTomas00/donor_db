import datetime
import os
import time
from pathlib import Path
from typing import List, Optional

import typer
from loguru import logger
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn,
                           TimeElapsedColumn)
from rich.prompt import Prompt
from rich.table import Table

from src.config import DATA_ROOT
from src.features.features import download_documents
from src.features.utils import load_params
from src.pipeline.dtx import dtx
from src.pipeline.extract import extract_data
from src.pipeline.load import insert_data
from src.pipeline.transform import transform_df

# Set up the app and console
app = typer.Typer(help="Donor DB Clinic - ETL Pipeline")
console = Console()

# Load parameters
extract_params = load_params("extract")
queries = extract_params["consultas"]


@app.command("extract")
def cmd_extract(
    start_date: str = typer.Option("01/01/2025", help="Fecha de inicio (DD/MM/YYYY)"),
    end_date: Optional[str] = typer.Option(None, help="Fecha de fin (DD/MM/YYYY) - Por defecto, fecha actual"),
    queries_list: Optional[List[str]] = typer.Option(None, help="Lista de consultas a extraer (separadas por comas)")
):
    """
    Extrae datos desde el web service de RSA.
    """
    console.print(Panel("🔍 [bold blue]Extracción de Datos[/bold blue]", expand=False))

    # Get dates
    start_date, end_date = _get_dates(start_date, end_date)

    # Get selected queries
    selected_queries = _get_selected_queries(queries_list)

    # Extract data
    dfs = _extract_data_from_queries(selected_queries, start_date, end_date)

    # Process and save data
    _process_and_save_data(dfs)


def _get_dates(start_date: str, end_date: Optional[str]) -> tuple[str, str]:
    """Get start and end dates from user input if needed."""
    start_date = Prompt.ask("Fecha de inicio (DD/MM/YYYY)", default=start_date)
    if not end_date:
        end_date = Prompt.ask("Fecha de fin (DD/MM/YYYY)",
                             default=datetime.datetime.now().strftime("%d/%m/%Y"))
    return start_date, end_date


def _get_selected_queries(queries_list: Optional[List[str]]) -> dict:
    """Get the queries to extract based on user input."""
    available_queries = list(queries.keys())
    selected_queries = {}

    if queries_list:
        # Process comma-separated list of query names
        queries_names = [c.strip() for c in queries_list.split(",")]
        for name in queries_names:
            if name in queries:
                selected_queries[name] = queries[name]
            else:
                console.print(f"[yellow]Advertencia: La consulta '{name}' no existe y será ignorada[/yellow]")
    else:
        # Show table of available queries
        _display_available_queries(available_queries)
        selected_queries = _select_queries_interactively(available_queries)

    return selected_queries


def _display_available_queries(available_queries: list) -> None:
    """Display a table with all available queries."""
    table = Table(title="Consultas Disponibles")
    table.add_column("Núm.", justify="center", style="cyan")
    table.add_column("Consulta", style="green")

    for i, query in enumerate(available_queries, 1):
        table.add_row(str(i), query)

    console.print(table)


def _select_queries_interactively(available_queries: list) -> dict:
    """Let the user select queries interactively."""
    selected_queries = {}
    selected_indices = Prompt.ask(
        "Seleccione las consultas a extraer (números separados por comas, 0 para todas)",
        default="0"
    )

    if selected_indices.strip() == "0":
        return queries

    try:
        indices = [int(i.strip()) for i in selected_indices.split(",")]
        for idx in indices:
            if 1 <= idx <= len(available_queries):
                name = available_queries[idx-1]
                selected_queries[name] = queries[name]
            else:
                console.print(f"[yellow]Advertencia: El índice {idx} está fuera de rango[/yellow]")
    except ValueError as exc:
        console.print("[bold red]Error: Los índices deben ser números[/bold red]")
        raise typer.Exit(1) from exc

    return selected_queries


def _extract_data_from_queries(selected_queries: dict, start_date: str, end_date: str) -> dict:
    """Extract data for each selected query."""
    dfs = {}

    for _, (name, query) in enumerate(selected_queries.items()):
        try:
            result = extract_data(queries={name: query}, start_date=start_date, end_date=end_date)
            dfs.update(result)
        except Exception as exc:
            console.print(f"[bold red]Error al extraer datos de {name}: {str(exc)}[/bold red]")

    console.print("\n[bold green]¡Datos extraídos correctamente![/bold green]")
    return dfs


def _process_and_save_data(dfs: dict) -> None:
    """Process, transform if requested, and save the extracted data."""
    transform = Prompt.ask(
        "Desea aplicar las transformaciones a los datos extraídos?",
        choices=["Si", "No"],
        default="No"
    )

    os.makedirs(DATA_ROOT / "queries", exist_ok=True)

    if transform == "Si":
        transform_type = Prompt.ask(
            "Seleccione el tipo de transformación",
            choices=["dense", "star"],
            default="dense"
        )

        transformed_dfs = _apply_transformation(dfs, transform_type)
        _save_dataframes(transformed_dfs, "transformed")
        _display_data_summary(transformed_dfs)
    else:
        _save_dataframes(dfs)
        _display_data_summary(dfs)

    logger.info("Datos guardados correctamente en ./queries/")


def _apply_transformation(dfs: dict, transform_type: str) -> dict:
    """Apply the selected transformation to the data."""
    if transform_type == "dense":
        return transform_df_dense(dfs)
    else:
        return transform_df_star(dfs)


def _save_dataframes(dataframes: dict, prefix: str = "") -> None:
    """Save dataframes to CSV files."""
    for name, df in dataframes.items():
        filename = f"{name}.csv" if not prefix else f"{name}_{prefix}.csv"
        df.to_csv(DATA_ROOT / "queries" / filename)


def _display_data_summary(dataframes: dict) -> None:
    """Display a summary table of the data."""
    table = Table(title="Resumen de Datos Extraídos")
    table.add_column("Consulta", style="green")
    table.add_column("Registros", justify="right", style="cyan")

    for name, df in dataframes.items():
        table.add_row(name, str(len(df)))

    console.print(table)


@app.command("download-documents")
def cmd_download_documents(
    start_date: str = typer.Option("01/01/2025", help="Fecha de inicio (DD/MM/YYYY)"),
    end_date: Optional[str] = typer.Option(None, help="Fecha de fin (DD/MM/YYYY)")
):
    """
    Descarga documentos desde el web service de RSA.
    """
    console.print(Panel("📄 [bold blue]Descarga de Documentos[/bold blue]", expand=False))

    if not end_date:
        end_date = datetime.datetime.now().strftime("%d/%m/%Y")
        console.print(f"[blue]Se utilizará la fecha actual como fecha de fin: {end_date}[/blue]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn()
    ) as progress:
        task = progress.add_task("[green]Descargando documentos...", total=1)

        try:
            documents_data = download_documents(start_date, end_date)
            if documents_data is not None:
                progress.update(task, completed=1)
                console.print("[green]✓ Documentos descargados correctamente[/green]")
            else:
                console.print("[yellow]No se encontraron documentos para descargar.[/yellow]")

        except Exception as exc:
            console.print(f"[bold red]Error al descargar documentos: {str(exc)}[/bold red]")
            raise typer.Exit(1) from exc

@app.command("pipeline")
def cmd_pipeline(
    pipeline_type: str = typer.Option("dense", help="Tipo de pipeline: dense o star"),
    start_date: str = typer.Option("01/01/2025", help="Fecha de inicio (DD/MM/YYYY)"),
    end_date: Optional[str] = typer.Option(None, help="Fecha de fin (DD/MM/YYYY)")
):
    """
    Ejecuta el pipeline completo de ETL.
    """
    console.print(Panel("🚀 [bold blue]Ejecutando Pipeline Completo[/bold blue]", expand=False))

    if not end_date:
        end_date = datetime.datetime.now().strftime("%d/%m/%Y")
        console.print(f"[blue]Se utilizará la fecha actual como fecha de fin: {end_date}[/blue]")

    if pipeline_type not in ["dense", "star"]:
        console.print("[bold red]Error: El tipo de pipeline debe ser 'dense' o 'star'[/bold red]")
        raise typer.Exit(1)

    # Create progress bar for overall progress
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn()
    ) as progress:
        overall_task = progress.add_task("[bold blue]Ejecutando pipeline completo...", total=100)
        transform_task = None
        load_task = None
        dfs = {}
        transformed_dfs = {}

        try:
            # 1. Extract data
            try:
                for _, (name, query) in enumerate(queries.items()):
                    try:
                        result = extract_data(queries={name: query}, start_date=start_date, end_date=end_date)
                        dfs.update(result)
                    except Exception as exc:
                        console.print(f"[bold red]Error al extraer datos de {name}: {str(exc)}[/bold red]")

                    # Save dataframes for future use
                    os.makedirs(DATA_ROOT / "tmp", exist_ok=True)
                    for df_name, df in dfs.items():
                        df.to_pickle(DATA_ROOT / "tmp" / f"{df_name}.pkl")

                console.print("[green]✓ Datos extraídos correctamente[/green]")
            except Exception as exc:
                console.print(f"[bold red]Error en extracción: {str(exc)}[/bold red]")
                raise typer.Exit(1) from exc

            progress.update(overall_task, advance=30)
            # 2. Transform data
            transform_task = progress.add_task("[green]Paso 2: Transformando datos...", total=100, visible=True)

            try:
                progress.update(transform_task, completed=33)
                time.sleep(1)  # To show the progress

                if pipeline_type == "dense":
                    transformed_dfs = transform_df_dense(dfs)
                else:
                    transformed_dfs = transform_df_star(dfs)

                progress.update(transform_task, completed=70)
                time.sleep(0.5)  # To show the progress

                # Save transformed dataframes
                os.makedirs(DATA_ROOT / "transformed", exist_ok=True)
                for name, df in transformed_dfs.items():
                    df.to_pickle(DATA_ROOT / "transformed" / f"{name}_{pipeline_type}.pkl")

                progress.update(transform_task, completed=100)
                console.print("[green]✓ Datos transformados correctamente[/green]")
                progress.update(overall_task, advance=30)
            except Exception as exc:
                console.print(f"[bold red]Error en transformación: {str(exc)}[/bold red]")
                raise typer.Exit(1) from exc

            # 3. Load data
            load_task = progress.add_task("[green]Paso 3: Cargando datos...", total=100, visible=True)

            try:
                insert_data(transformed_dfs)
                progress.update(load_task, completed=100)
            except typer.Exit:
                raise
            except Exception as exc:
                console.print(f"[bold red]Error al cargar datos: {str(exc)}[/bold red]")
                raise typer.Exit(1) from exc

            progress.update(overall_task, completed=100)

        except typer.Exit:
            progress.update(overall_task, visible=False)
            raise
        except Exception as exc:
            progress.update(overall_task, visible=False)
            console.print(f"[bold red]Error inesperado en el pipeline: {str(exc)}[/bold red]")
            raise typer.Exit(1) from exc

    # Print summary
    console.print("\n[bold green]¡Pipeline ejecutado correctamente![/bold green]")

    # Create a table with general metrics
    general_table = Table(title="Resumen General del Pipeline")
    general_table.add_column("Métrica", style="green")
    general_table.add_column("Valor", style="cyan")

    general_table.add_row("Tipo de Pipeline", pipeline_type)
    general_table.add_row("Consultas procesadas", str(len(dfs)))
    general_table.add_row("Tablas creadas", str(len(transformed_dfs)))
    general_table.add_row("Total de registros", str(sum(len(df) for df in transformed_dfs.values())))

    console.print(general_table)

    # Create a detailed table showing records per table
    details_table = Table(title="Registros por Tabla")
    details_table.add_column("Tabla", style="green")
    details_table.add_column("Registros", justify="right", style="cyan")
    details_table.add_column("Columnas", justify="right", style="blue")

    for name, df in transformed_dfs.items():
        details_table.add_row(name, str(len(df)), str(len(df.columns)))

    console.print(details_table)

@app.callback()
def main():
    """
    Donor DB Clinic - Herramienta ETL para la gestión de datos de donantes.
    """
    rprint("[bold blue]╔════════════════════════════════════════════════════════════╗[/bold blue]")
    rprint("[bold blue]║                 [bold]🏥 DONOR DB CLINIC[/bold][bold blue] ║[/bold blue]")
    rprint("[bold blue]╚════════════════════════════════════════════════════════════╝[/bold blue]")
    rprint("[dim]Pipeline ETL para la gestión de datos de donantes[/dim]\n")


if __name__ == "__main__":
    app()
