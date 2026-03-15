#!/usr/bin/env python3
"""
Flag high-employment occupation rows whose SOC mapping may need review.

This script is intentionally heuristic. It does not auto-correct mappings; it
creates a ranked review file so manual checks focus on the highest-impact rows.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List


STOPWORDS = {
    "and",
    "for",
    "the",
    "of",
    "in",
    "to",
    "with",
    "on",
    "or",
    "a",
    "an",
    "other",
    "professional",
    "professionals",
    "assistant",
    "assistants",
    "worker",
    "workers",
}


def to_float(value: str) -> float | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def tokenize(text: str) -> set[str]:
    tokens = set(re.findall(r"[a-z0-9]+", (text or "").lower()))
    return {t for t in tokens if len(t) > 2 and t not in STOPWORDS}


def token_overlap_ratio(title: str, label: str) -> float:
    t1 = tokenize(title)
    t2 = tokenize(label)
    if not t1:
        return 0.0
    return len(t1.intersection(t2)) / len(t1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit high-job SOC mappings")
    parser.add_argument("--min-jobs", type=int, default=20000)
    parser.add_argument("--top", type=int, default=200)
    parser.add_argument("--site-data", default="site/data.json")
    parser.add_argument("--crosswalk", default="data/soc_crosswalk.csv")
    parser.add_argument("--out", default="data/manual_review_high_jobs.csv")
    parser.add_argument(
        "--accepted",
        default="data/manual_review_high_jobs_accepted.csv",
        help="CSV of previously accepted slugs to suppress from audit output.",
    )
    args = parser.parse_args()

    root = Path(".")
    site_payload = json.loads((root / args.site_data).read_text())
    if isinstance(site_payload, list):
        rows = site_payload
    else:
        rows = site_payload.get("occupations_estimated") or site_payload.get("occupations") or []

    by_slug = {r["slug"]: r for r in rows if r.get("slug")}
    accepted_slugs = set()
    accepted_path = root / args.accepted
    if accepted_path.exists():
        with accepted_path.open(newline="") as f:
            for row in csv.DictReader(f):
                slug = (row.get("slug") or "").strip()
                if slug:
                    accepted_slugs.add(slug)

    candidates: List[dict] = []
    with (root / args.crosswalk).open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = (row.get("slug") or "").strip()
            if slug in accepted_slugs:
                continue
            if slug not in by_slug:
                continue
            occ = by_slug[slug]
            jobs = int(occ.get("jobs") or 0)
            if jobs < args.min_jobs:
                continue

            reasons: List[str] = []
            match_score = to_float(row.get("match_score", ""))
            api_score = to_float(row.get("api_score", ""))
            llm_conf = to_float(row.get("llm_confidence", ""))
            method = (row.get("api_match_method") or "").strip()
            matched_label = (row.get("matched_label") or "").strip()

            if match_score is not None and match_score < 90:
                reasons.append(f"low_match_score:{match_score:.1f}")
            if api_score is not None and api_score < 95:
                reasons.append(f"low_api_score:{api_score:.1f}")
            if llm_conf is not None and llm_conf < 0.80:
                reasons.append(f"low_llm_conf:{llm_conf:.2f}")
            if "manual_review" in method or "override" in method or "manual" in method:
                reasons.append(f"manual_method:{method}")

            overlap = token_overlap_ratio(occ.get("title", ""), matched_label)
            # Only use weak title/label overlap as an additional signal.
            # On its own it creates many false positives (for example plural forms
            # or broad SOC labels that are still correct).
            if matched_label and overlap < 0.35 and reasons:
                reasons.append(f"title_label_overlap:{overlap:.2f}")

            if reasons:
                candidates.append(
                    {
                        "slug": slug,
                        "title": occ.get("title", ""),
                        "jobs": jobs,
                        "soc_code": row.get("soc_code", ""),
                        "matched_label": matched_label,
                        "match_score": row.get("match_score", ""),
                        "api_score": row.get("api_score", ""),
                        "llm_confidence": row.get("llm_confidence", ""),
                        "api_match_method": method,
                        "reasons": ";".join(reasons),
                    }
                )

    candidates.sort(key=lambda r: int(r["jobs"]), reverse=True)
    candidates = candidates[: args.top]

    out_path = root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "slug",
        "title",
        "jobs",
        "soc_code",
        "matched_label",
        "match_score",
        "api_score",
        "llm_confidence",
        "api_match_method",
        "reasons",
    ]
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(candidates)

    print(f"Wrote {out_path} with {len(candidates)} flagged rows (min_jobs={args.min_jobs})")


if __name__ == "__main__":
    main()
