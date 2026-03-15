#!/usr/bin/env python3
"""
Convert scraped UK occupation HTML files into markdown pages.

Reads:
- occupations.json
- html/<slug>.html

Writes:
- pages/<slug>.md
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import List

from bs4 import BeautifulSoup


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_profile_to_markdown(html_path: Path, fallback_title: str, source_url: str) -> str:
    soup = BeautifulSoup(html_path.read_text(), "html.parser")
    md: List[str] = []

    h1 = soup.find("h1")
    title = clean(h1.get_text()) if h1 else fallback_title
    md.append(f"# {title}")
    md.append("")
    md.append(f"**Source:** {source_url}")
    md.append("")

    main = soup.find("main") or soup

    # Pull top summary paragraph if available.
    lead = main.select_one("p.govuk-body-l, p.govuk-body")
    if lead:
        lead_txt = clean(lead.get_text())
        if lead_txt:
            md.append(lead_txt)
            md.append("")

    # Generic section extraction.
    for heading in main.select("h2, h3"):
        h_text = clean(heading.get_text())
        if not h_text:
            continue
        level = "##" if heading.name == "h2" else "###"
        md.append(f"{level} {h_text}")
        md.append("")

        # Include sibling content until next heading at same/higher level.
        for sib in heading.find_next_siblings():
            if sib.name in ("h2", "h3"):
                break
            if sib.name == "p":
                txt = clean(sib.get_text())
                if txt:
                    md.append(txt)
                    md.append("")
            elif sib.name in ("ul", "ol"):
                for li in sib.find_all("li"):
                    txt = clean(li.get_text())
                    if txt:
                        md.append(f"- {txt}")
                md.append("")
            elif sib.name == "table":
                rows = sib.find_all("tr")
                table_data = []
                for row in rows:
                    cells = [clean(c.get_text()) for c in row.find_all(["th", "td"])]
                    if any(cells):
                        table_data.append(cells)
                if table_data:
                    width = max(len(r) for r in table_data)
                    for r in table_data:
                        while len(r) < width:
                            r.append("")
                    md.append("| " + " | ".join(table_data[0]) + " |")
                    md.append("| " + " | ".join(["---"] * width) + " |")
                    for r in table_data[1:]:
                        md.append("| " + " | ".join(r) + " |")
                    md.append("")

    return "\n".join(md).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Process UK HTML pages into markdown")
    parser.add_argument("--force", action="store_true", help="Re-process existing markdown files")
    args = parser.parse_args()

    root = Path(".")
    pages_dir = root / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    occupations = json.loads((root / "occupations.json").read_text())
    processed = 0
    cached = 0
    missing = 0

    for occ in occupations:
        slug = occ["slug"]
        html_path = root / "html" / f"{slug}.html"
        md_path = pages_dir / f"{slug}.md"

        if not html_path.exists():
            missing += 1
            continue
        if md_path.exists() and not args.force:
            cached += 1
            continue

        markdown = parse_profile_to_markdown(
            html_path=html_path,
            fallback_title=occ["title"],
            source_url=occ["url"],
        )
        md_path.write_text(markdown)
        processed += 1

    print(f"Processed: {processed}")
    print(f"Cached: {cached}")
    print(f"Missing HTML: {missing}")
    print(f"Total markdown files: {len(list(pages_dir.glob('*.md')))}")


if __name__ == "__main__":
    main()
