#!/usr/bin/env python3
"""
Scrape UK occupation pages from National Careers Service.

Outputs:
- occupations.json
- html/<slug>.html
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


BASE_URL = "https://nationalcareers.service.gov.uk"
ALL_CAREERS_URL = f"{BASE_URL}/explore-careers/all-careers"
DEFAULT_DELAY = 0.35
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF = 1.5
DEFAULT_MIN_OCCUPATIONS = 100


@dataclass
class Occupation:
    title: str
    url: str
    category: str
    slug: str


def slugify(text: str) -> str:
    text = unescape(text).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def unique_slug(slug: str, seen: Dict[str, int]) -> str:
    if slug not in seen:
        seen[slug] = 1
        return slug
    seen[slug] += 1
    return f"{slug}-{seen[slug]}"


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape UK careers data")
    parser.add_argument("--force", action="store_true", help="Re-download existing html pages")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between requests")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retry attempts on failed page fetch")
    parser.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF, help="Seconds for linear backoff between retries")
    parser.add_argument("--max", type=int, default=None, help="Limit number of occupations for testing")
    parser.add_argument(
        "--min-occupations",
        type=int,
        default=DEFAULT_MIN_OCCUPATIONS,
        help="Fail fast if fewer than this many occupation links are discovered",
    )
    return parser


def make_client(timeout: int) -> httpx.Client:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-GB,en;q=0.9",
    }
    return httpx.Client(headers=headers, timeout=timeout, follow_redirects=True)


def parse_occupation_links(html: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[Tuple[str, str]] = []
    seen = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        parsed = urlparse(href)
        path = parsed.path
        query = parsed.query.lower()

        # Primary source: NCS job profile URLs.
        if path.startswith("/job-profiles/"):
            if path in ("/job-profiles", "/job-profiles/"):
                continue
        # Secondary source: direct explore-careers profile links, but never index/pagination.
        elif path.startswith("/explore-careers/"):
            if path.startswith("/explore-careers/all-careers"):
                continue
            if path in ("/explore-careers", "/explore-careers/"):
                continue
        else:
            continue

        # Skip obvious pagination/query links.
        if "page=" in query:
            continue

        full_url = urljoin(BASE_URL, path)
        title = re.sub(r"\s+", " ", a.get_text(" ", strip=True))
        if re.fullmatch(r"\d+", title) or "next" in title.lower():
            # Pagination labels should never be occupation titles.
            continue
        if not title:
            slug = path.rsplit("/", 1)[-1]
            title = slug.replace("-", " ").title()
        key = (title, full_url)
        if key in seen:
            continue
        seen.add(key)
        links.append(key)
    return links


def parse_catalog_pagination_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    pages: List[str] = []
    seen = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        parsed = urlparse(href)
        path = parsed.path
        if path.rstrip("/") != "/explore-careers/all-careers":
            continue
        full = urljoin(BASE_URL, href)
        if full in seen:
            continue
        seen.add(full)
        pages.append(full)
    return pages


def parse_category(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    # Look for breadcrumb item before current page.
    crumbs = soup.select("nav[aria-label*='Breadcrumb'] a, .govuk-breadcrumbs a")
    for crumb in reversed(crumbs):
        txt = re.sub(r"\s+", " ", crumb.get_text(" ", strip=True))
        if not txt:
            continue
        low = txt.lower()
        if "explore careers" in low:
            continue
        return slugify(txt) or None

    # Fallback: any "job category" text in metadata lists.
    for dt in soup.select("dt"):
        label = dt.get_text(" ", strip=True).lower()
        if "job category" in label or "category" == label:
            dd = dt.find_next("dd")
            if dd:
                return slugify(dd.get_text(" ", strip=True)) or None

    return None


def fetch_text(client: httpx.Client, url: str) -> str:
    r = client.get(url)
    r.raise_for_status()
    return r.text


def fetch_text_with_retry(client: httpx.Client, url: str, retries: int, backoff: float) -> str:
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            return fetch_text(client, url)
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            sleep_for = backoff * (attempt + 1)
            time.sleep(sleep_for)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("All retry attempts failed")


def main() -> None:
    args = get_parser().parse_args()
    root = Path(".")
    html_dir = root / "html"
    html_dir.mkdir(parents=True, exist_ok=True)

    client = make_client(args.timeout)
    try:
        print(f"Fetching index: {ALL_CAREERS_URL}")
        first_index_html = fetch_text_with_retry(
            client, ALL_CAREERS_URL, retries=args.retries, backoff=args.backoff
        )
        # Crawl all catalogue pagination URLs to capture the full occupation list.
        index_queue = [ALL_CAREERS_URL]
        index_seen = {ALL_CAREERS_URL}
        index_html_by_url: Dict[str, str] = {ALL_CAREERS_URL: first_index_html}

        for page_url in parse_catalog_pagination_links(first_index_html):
            if page_url not in index_seen:
                index_seen.add(page_url)
                index_queue.append(page_url)

        i = 0
        while i < len(index_queue):
            page_url = index_queue[i]
            if page_url not in index_html_by_url:
                try:
                    page_html = fetch_text_with_retry(
                        client, page_url, retries=args.retries, backoff=args.backoff
                    )
                    index_html_by_url[page_url] = page_html
                except Exception as exc:
                    print(f"WARN: failed to fetch catalogue page {page_url}: {exc}")
                    i += 1
                    if i < len(index_queue):
                        time.sleep(args.delay)
                    continue
            if i + 1 < len(index_queue):
                time.sleep(args.delay)

        for discovered in parse_catalog_pagination_links(index_html_by_url[page_url]):
                if discovered not in index_seen:
                    index_seen.add(discovered)
                    index_queue.append(discovered)
            i += 1

        links: List[Tuple[str, str]] = []
        link_seen = set()
        for html in index_html_by_url.values():
            for title, url in parse_occupation_links(html):
                key = (title, url)
                if key in link_seen:
                    continue
                link_seen.add(key)
                links.append((title, url))

        print(f"Discovered {len(index_html_by_url)} catalogue pages")
        if not links:
            raise RuntimeError(
                "No occupation links found from all-careers page. "
                "Site may be blocked or structure changed."
            )
        if len(links) < args.min_occupations and args.max is None:
            raise RuntimeError(
                f"Only discovered {len(links)} occupation links (< {args.min_occupations}). "
                "Likely page-structure mismatch or blocked content."
            )
        print(f"Discovered {len(links)} occupation links")

        if args.max is not None:
            links = links[: args.max]
            print(f"Limited to first {len(links)} due to --max")

        occupations: List[Occupation] = []
        seen_slug: Dict[str, int] = {}

        for idx, (title, url) in enumerate(links, start=1):
            raw_slug = slugify(title) or url.rsplit("/", 1)[-1]
            slug = unique_slug(raw_slug, seen_slug)
            out_html = html_dir / f"{slug}.html"

            page_html = ""
            if out_html.exists() and not args.force:
                page_html = out_html.read_text()
                status = "CACHED"
            else:
                try:
                    page_html = fetch_text_with_retry(
                        client, url, retries=args.retries, backoff=args.backoff
                    )
                    out_html.write_text(page_html)
                    status = "OK"
                except Exception as exc:
                    status = f"ERROR: {exc}"
                    print(f"[{idx}/{len(links)}] {title} -> {status}")
                    if idx < len(links):
                        time.sleep(args.delay)
                    continue

            category = parse_category(page_html) or "uncategorized"
            occupations.append(Occupation(title=title, url=url, category=category, slug=slug))
            print(f"[{idx}/{len(links)}] {title} -> {status}")
            if idx < len(links):
                time.sleep(args.delay)

        occupations_payload = [
            {
                "title": occ.title,
                "url": occ.url,
                "category": occ.category,
                "slug": occ.slug,
            }
            for occ in occupations
        ]

        with open("occupations.json", "w") as f:
            json.dump(occupations_payload, f, indent=2)

        print(f"\nWrote occupations.json ({len(occupations_payload)} occupations)")
        print(f"HTML cache directory: {html_dir.resolve()}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
