#!/usr/bin/env python3
"""
Fetch all Tushare API documents and save them into categorized text files.
Requires: requests, beautifulsoup4 (available in base environment).
Optional: set env TUSHARE_PROXY (e.g. http://127.0.0.1:10808) to route via a proxy.
"""

from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

BASE_URL = "https://tushare.pro/document/2"
OUTPUT_DIR = Path("tushare_api_docs")
REQUEST_TIMEOUT = 30
PROXY_URL = os.environ.get("TUSHARE_PROXY")


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) TushareScraper/1.0"}
    )
    session.trust_env = False
    if PROXY_URL:
        session.proxies.update({"http": PROXY_URL, "https": PROXY_URL})
    return session


def fetch_html(url: str, session: requests.Session) -> str:
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(1)
    raise RuntimeError("Unreachable")


def normalize_space(text: str) -> str:
    return re.sub(r"[ \t]+", " ", text).strip()


def format_table(table: Tag) -> str:
    rows = []
    for tr in table.find_all("tr"):
        cells = [
            normalize_space(cell.get_text(" ", strip=True))
            for cell in tr.find_all(["th", "td"])
        ]
        if cells:
            rows.append(cells)
    if not rows:
        return ""
    max_cols = max(len(r) for r in rows)
    for r in rows:
        if len(r) < max_cols:
            r.extend([""] * (max_cols - len(r)))
    col_widths = [max(len(r[i]) for r in rows) for i in range(max_cols)]

    def fmt(row: Iterable[str]) -> str:
        return " | ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row))

    divider = "-+-".join("-" * w for w in col_widths)
    lines = [fmt(rows[0]), divider]
    lines.extend(fmt(r) for r in rows[1:])
    return "\n".join(lines)


def format_list(lst: Tag) -> List[str]:
    bullet = "-" if lst.name == "ul" else "1."
    lines = []
    for li in lst.find_all("li", recursive=False):
        text = normalize_space(li.get_text(" ", strip=True))
        if text:
            lines.append(f"{bullet} {text}")
    return lines


def node_to_text(node: Tag) -> List[str]:
    lines: List[str] = []
    if isinstance(node, NavigableString):
        return []
    name = node.name
    if name in {"h1", "h2", "h3"}:
        level = {"h1": "#", "h2": "##", "h3": "###"}[name]
        text = normalize_space(node.get_text(" ", strip=True))
        if text:
            lines.append(f"{level} {text}")
    elif name == "p":
        text = normalize_space(node.get_text(" ", strip=True))
        if text:
            lines.append(text)
    elif name == "table":
        table_text = format_table(node)
        if table_text:
            lines.append(table_text)
    elif name in {"ul", "ol"}:
        lines.extend(format_list(node))
    elif name == "pre":
        code_text = node.get_text("\n", strip=True)
        lines.append("```")
        lines.append(code_text)
        lines.append("```")
    elif name == "hr":
        lines.append("-" * 40)
    else:
        text = normalize_space(node.get_text(" ", strip=True))
        if text:
            lines.append(text)
    return lines


def parse_doc_text(html: str, doc_id: str, path: List[str]) -> str:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("div.content")
    title = " / ".join([p for p in path if p])
    header = f"# {title} (doc_id={doc_id})" if title else f"# doc_id={doc_id}"
    if not content:
        return f"{header}\n(No content found)\n"
    lines: List[str] = [header, ""]
    for child in content.children:
        if isinstance(child, NavigableString):
            continue
        if child.get("class") == ["search-panel"]:
            continue
        lines.extend(node_to_text(child))
        if lines and lines[-1] != "":
            lines.append("")
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines) + "\n"


def safe_filename(name: str) -> str:
    cleaned = re.sub(r"[\\\\/:*?\"<>|]", "_", name).strip()
    return cleaned or "untitled"


def extract_doc_entries(html: str) -> List[Dict[str, List[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#jstree > ul") or soup.select_one("#jstree")
    entries: List[Dict[str, List[str]]] = []
    seen = set()

    def walk(li: Tag, ancestors: List[str]) -> None:
        a = li.find("a", recursive=False)
        if not a:
            return
        title = normalize_space(a.get_text(" ", strip=True))
        href = a.get("href") or ""
        match = re.search(r"doc_id=(\d+)", href)
        doc_id = match.group(1) if match else None
        path = ancestors + [title]
        if doc_id and doc_id not in seen:
            entries.append({"doc_id": doc_id, "path": path})
            seen.add(doc_id)
        child_ul = li.find("ul", recursive=False)
        if child_ul:
            for child_li in child_ul.find_all("li", recursive=False):
                walk(child_li, path)

    if container:
        if container.name == "ul":
            li_list = container.find_all("li", recursive=False)
        else:
            li_list = container.select("ul > li")
        for li in li_list:
            walk(li, [])
    return entries


def main() -> int:
    session = build_session()
    OUTPUT_DIR.mkdir(exist_ok=True)

    print(f"Fetching doc index from {BASE_URL} ...")
    index_html = fetch_html(BASE_URL, session)
    entries = extract_doc_entries(index_html)
    print(f"Found {len(entries)} doc entries.")

    for idx, entry in enumerate(entries, 1):
        url = f"{BASE_URL}?doc_id={entry['doc_id']}"
        label = " / ".join(entry["path"])
        print(f"[{idx}/{len(entries)}] Fetching doc_id={entry['doc_id']} ({label}) ...")
        html = fetch_html(url, session)
        content = parse_doc_text(html, entry["doc_id"], entry["path"])

        parts = [safe_filename(p) for p in entry["path"][:-1]]
        dir_path = OUTPUT_DIR / Path("/".join(parts)) if parts else OUTPUT_DIR
        dir_path.mkdir(parents=True, exist_ok=True)

        leaf = entry["path"][-1] if entry["path"] else "doc"
        fname = safe_filename(f"{leaf}_docid{entry['doc_id']}.txt")
        out_path = dir_path / fname
        out_path.write_text(content, encoding="utf-8")

    print(f"Done. Files are stored in {OUTPUT_DIR.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
