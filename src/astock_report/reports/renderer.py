"""Report rendering helpers using Jinja2 templates."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, FileSystemLoader, StrictUndefined


@dataclass
class ReportRenderer:
    """Render reports from structured workflow outputs."""

    template_dir: Path
    template_name: str = "base_report.md.j2"

    def __post_init__(self) -> None:
        self._env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=False,
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render(self, context: Dict[str, Any]) -> str:
        """Render the configured template with supplied context."""
        return self.render_template(self.template_name, context)

    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        template = self._env.get_template(template_name)
        return template.render(**context)
