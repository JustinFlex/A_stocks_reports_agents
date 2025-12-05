"""CLI command definitions for the research report generator."""
from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import typer
from rich.console import Console
from rich.table import Table

from config import Config
from astock_report.utils.logging import configure_logging
from astock_report.workflows.graph import ReportWorkflow
from astock_report.workflows.state import ReportState

console = Console()
app = typer.Typer(help="Generate deep-dive A-share equity research reports from the terminal.")


@dataclass
class AppContext:
    """Holds reusable process-wide objects for CLI commands."""

    config: Config
    workflow: ReportWorkflow


def _init_context(debug_override: Optional[bool] = None) -> AppContext:
    """Create a context with configuration, logging, and workflow wiring."""
    config = Config.from_env()
    if debug_override is not None:
        config.debug = debug_override
    configure_logging(debug=config.debug)
    workflow = ReportWorkflow(config=config)
    return AppContext(config=config, workflow=workflow)


@app.callback()
def main_callback(
    ctx: typer.Context,
    debug: Optional[bool] = typer.Option(
        None,
        "--debug/--no-debug",
        help="Temporarily toggle verbose logging without touching environment variables.",
    ),
) -> None:
    """Attach the lazily constructed application context to Typer."""
    ctx.obj = _init_context(debug_override=debug)


@app.command()
def generate(
    ctx: typer.Context,
    ticker: str = typer.Argument(..., help="A-share ticker, e.g. 600000.SH"),
    name: Optional[str] = typer.Option(None, "--name", help="Optional company display name."),
    emit_json: bool = typer.Option(False, "--json", help="Persist the merged workflow state to JSON."),
    markdown_path: Optional[Path] = typer.Option(
        None,
        "--markdown",
        help="Optional custom path for the rendered Markdown report.",
    ),
    pdf: bool = typer.Option(False, "--pdf", help="Generate PDF via pandoc if available."),
    pdf_source: str = typer.Option(
        "md",
        "--pdf-source",
        help="Choose pdf source: md (Markdown) or html (HTML template)",
        case_sensitive=False,
    ),
    # Valuation overrides
    wacc: Optional[float] = typer.Option(None, "--wacc", help="Override WACC for DCF (e.g., 0.09)."),
    growth: Optional[float] = typer.Option(None, "--growth", help="Override near-term FCF growth rate g (e.g., 0.07)."),
    terminal_growth: Optional[float] = typer.Option(
        None, "--terminal-growth", help="Override terminal growth gt for DCF (e.g., 0.03)."
    ),
    forecast_years: Optional[int] = typer.Option(None, "--forecast-years", help="Override DCF forecast years."),
    pe_low: Optional[float] = typer.Option(None, "--pe-low", help="Lower bound for PE band valuation."),
    pe_high: Optional[float] = typer.Option(None, "--pe-high", help="Upper bound for PE band valuation."),
    ev_ebitda_low: Optional[float] = typer.Option(None, "--ev-ebitda-low", help="Lower bound for EV/EBITDA band."),
    ev_ebitda_high: Optional[float] = typer.Option(None, "--ev-ebitda-high", help="Upper bound for EV/EBITDA band."),
    pb_low: Optional[float] = typer.Option(None, "--pb-low", help="Lower bound for PB band valuation."),
    pb_high: Optional[float] = typer.Option(None, "--pb-high", help="Upper bound for PB band valuation."),
    ev_sales_low: Optional[float] = typer.Option(None, "--ev-sales-low", help="Lower bound for EV/Sales band."),
    ev_sales_high: Optional[float] = typer.Option(None, "--ev-sales-high", help="Upper bound for EV/Sales band."),
    # LLM overrides
    news_web_search: Optional[bool] = typer.Option(
        None, "--news-web-search/--no-news-web-search", help="Force enable/disable web_search for news node."
    ),
    qual_web_search: Optional[bool] = typer.Option(
        None, "--qual-web-search/--no-qual-web-search", help="Force enable/disable web_search for qual node."
    ),
    news_thinking_budget: Optional[int] = typer.Option(
        None, "--news-thinking-budget", help="Override thinking_budget for news Map/Reduce call."
    ),
    qual_thinking_budget: Optional[int] = typer.Option(
        None, "--qual-thinking-budget", help="Override thinking_budget for qual research call."
    ),
) -> None:
    """Run the LangGraph workflow for a single ticker and present the outcome."""
    if ctx.obj is None:
        raise typer.Exit(code=1)

    context: AppContext = ctx.obj
    console.rule(f"Generating report for {ticker}")

    valuation_overrides = {
        key: value
        for key, value in {
            "wacc": wacc,
            "g": growth,
            "terminal_growth": terminal_growth,
            "forecast_years": forecast_years,
            "pe_low": pe_low,
            "pe_high": pe_high,
            "ev_ebitda_low": ev_ebitda_low,
            "ev_ebitda_high": ev_ebitda_high,
            "pb_low": pb_low,
            "pb_high": pb_high,
            "ev_sales_low": ev_sales_low,
            "ev_sales_high": ev_sales_high,
        }.items()
        if value is not None
    }
    llm_overrides = {
        key: value
        for key, value in {
            "news_web_search": news_web_search,
            "qual_web_search": qual_web_search,
            "news_thinking_budget": news_thinking_budget,
            "qual_thinking_budget": qual_thinking_budget,
        }.items()
        if value is not None
    }

    with console.status("[bold cyan]Running workflow..."):
        result: ReportState = context.workflow.run(
            ticker=ticker,
            company_name=name,
            valuation_overrides=valuation_overrides or None,
            llm_overrides=llm_overrides or None,
        )

    if result.get("errors"):
        console.print("[bold red]Workflow completed with errors:[/bold red]")
        for issue in result["errors"]:
            console.print(f"- {issue}")
    else:
        console.print("[bold green]Workflow completed successfully.[/bold green]")

    _print_run_summary(result)

    if emit_json:
        target = markdown_path or context.config.output_dir / f"{ticker}_state.json"
        context.workflow.persist_state(result, target)
        console.print(f"State saved to {target}")

    pdf_source = pdf_source.lower()
    if pdf_source not in {"md", "html"}:
        console.print("[yellow]Invalid pdf-source; defaulting to md[/yellow]")
        pdf_source = "md"

    if result.get("markdown_report"):
        output_md = markdown_path or context.config.output_dir / f"{ticker}.md"
        context.workflow.persist_markdown(result["markdown_report"], output_md)
        console.print(f"Markdown report available at {output_md}")
        if pdf and pdf_source == "md":
            _render_pdf(output_md, console)

    if result.get("html_report"):
        output_html = context.config.output_dir / f"{ticker}.html"
        context.workflow.persist_html(result["html_report"], output_html)
        console.print(f"HTML report available at {output_html}")
        if pdf and pdf_source == "html":
            _render_pdf(output_html, console)


def _render_pdf(source_path: Path, console: Console) -> None:
    pdf_path = source_path.with_suffix(".pdf")
    if not shutil.which("pandoc"):
        console.print("[yellow]pandoc not found; skipping PDF generation[/yellow]")
        return
    extra_args = []
    if shutil.which("wkhtmltopdf"):
        extra_args = ["--pdf-engine=wkhtmltopdf", "-V", "margin-left=20mm", "-V", "margin-right=20mm", "-V", "margin-top=18mm", "-V", "margin-bottom=18mm"]
    try:
        subprocess.run(["pandoc", str(source_path), "-o", str(pdf_path), *extra_args], check=True)
        console.print(f"PDF generated at {pdf_path}")
    except subprocess.CalledProcessError as exc:
        console.print(f"[red]PDF generation failed: {exc}[/red]")


@app.command()
def batch(
    ctx: typer.Context,
    tickers: List[str] = typer.Argument(..., help="One or more tickers, e.g. 600000.SH 000001.SZ"),
    name: Optional[str] = typer.Option(None, "--name", help="Optional company display name applied to all."),
    pdf: bool = typer.Option(False, "--pdf", help="Generate PDF via pandoc if available."),
) -> None:
    """Run the workflow for multiple tickers sequentially."""
    if ctx.obj is None:
        raise typer.Exit(code=1)
    context: AppContext = ctx.obj
    for tk in tickers:
        console.rule(f"Batch generating {tk}")
        result: ReportState = context.workflow.run(ticker=tk, company_name=name)
        if result.get("markdown_report"):
            output_md = context.config.output_dir / f"{tk}.md"
            context.workflow.persist_markdown(result["markdown_report"], output_md)
            console.print(f"Markdown report available at {output_md}")
            if pdf:
                _render_pdf(output_md)
        if result.get("errors"):
            console.print(f"[yellow]Completed with errors for {tk}: {result['errors']}[/yellow]")


@app.command()
def plan(ctx: typer.Context) -> None:
    """Display the high-level workflow path for quick operator reference."""
    if ctx.obj is None:
        raise typer.Exit(code=1)

    context: AppContext = ctx.obj
    table = Table(title="Workflow Stages")
    table.add_column("Step", style="cyan")
    table.add_column("Description")

    for idx, step in enumerate(context.workflow.describe_stages(), start=1):
        table.add_row(str(idx), step)

    console.print(table)


def _print_run_summary(state: ReportState) -> None:
    """Pretty-print a short run summary for operators."""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Key")
    table.add_column("Value")

    table.add_row("Ticker", state.get("ticker", "?"))
    table.add_row("Company", state.get("company_name") or "N/A")
    table.add_row("Report Date", state.get("report_date") or "N/A")
    table.add_row("Has Financials", "yes" if state.get("financials") is not None else "no")
    table.add_row("Markdown", "yes" if state.get("markdown_report") else "no")
    table.add_row("Errors", str(len(state.get("errors", []))))

    console.print(table)
