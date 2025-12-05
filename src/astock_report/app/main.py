"""Application entry point for console execution."""
from __future__ import annotations

from astock_report.cli.commands import app


def main() -> None:
    """Invoke the Typer application."""
    app()


if __name__ == "__main__":
    main()