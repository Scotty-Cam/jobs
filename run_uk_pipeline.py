#!/usr/bin/env python3
"""
Run the UK jobs pipeline end-to-end with resumable stages.

This wrapper is intentionally lightweight:
- It auto-detects stage scripts if they exist in the current directory.
- It runs stages in order (capture -> process -> tabulate -> score -> build).
- It performs a basic output validation gate at the end.

Expected stage scripts (UK-specific names preferred):
  capture:  scrape_uk.py or scrape.py
  process:  process_uk.py or process.py
  tabulate: make_csv_uk.py or make_csv.py
  score:    score_uk.py or score.py
  build:    build_site_data_uk.py or build_site_data.py
"""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional


STAGE_ORDER = ["capture", "process", "tabulate", "score", "build"]

STAGE_CANDIDATES: Dict[str, List[str]] = {
    "capture": ["scrape_uk.py", "scrape.py"],
    "process": ["process_uk.py", "process.py"],
    "tabulate": ["make_csv_uk.py", "make_csv.py"],
    "score": ["score_uk.py", "score.py"],
    "build": ["build_site_data_uk.py", "build_site_data.py"],
}

EXPECTED_CSV_COLUMNS = [
    "title",
    "category",
    "slug",
    "soc_code",
    "median_pay_annual",
    "median_pay_hourly",
    "entry_education",
    "work_experience",
    "training",
    "num_jobs_2024",
    "projected_employment_2034",
    "outlook_pct",
    "outlook_desc",
    "employment_change",
    "url",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run UK jobs ETL + scoring + site data build."
    )
    parser.add_argument(
        "--runner",
        default="uv run python",
        help="Command used to run Python scripts (default: 'uv run python').",
    )
    parser.add_argument(
        "--from-stage",
        choices=STAGE_ORDER,
        default=STAGE_ORDER[0],
        help="First stage to run.",
    )
    parser.add_argument(
        "--to-stage",
        choices=STAGE_ORDER,
        default=STAGE_ORDER[-1],
        help="Last stage to run.",
    )
    parser.add_argument(
        "--skip-score",
        action="store_true",
        help="Skip scoring stage even if in selected stage range.",
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip final validation checks.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Run only validation checks on existing outputs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing.",
    )
    return parser.parse_args()


def stage_range(from_stage: str, to_stage: str) -> List[str]:
    start = STAGE_ORDER.index(from_stage)
    end = STAGE_ORDER.index(to_stage)
    if start > end:
        raise ValueError("--from-stage must come before --to-stage")
    return STAGE_ORDER[start : end + 1]


def detect_stage_script(stage: str, root: Path) -> Optional[Path]:
    for candidate in STAGE_CANDIDATES[stage]:
        candidate_path = root / candidate
        if candidate_path.exists():
            return candidate_path
    return None


def run_stage(stage: str, script_path: Path, runner: str, dry_run: bool) -> None:
    base_cmd = shlex.split(runner)
    cmd = base_cmd + [str(script_path)]
    printable = " ".join(shlex.quote(part) for part in cmd)
    print(f"\n[{stage}] {printable}")
    if dry_run:
        return
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Stage '{stage}' failed with exit code {result.returncode}")


def load_json_array(path: Path) -> List[dict]:
    with path.open() as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} is not a JSON array")
    return data


def load_site_rows(path: Path) -> List[dict]:
    with path.open() as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        raise ValueError(f"{path} is neither JSON array nor object")

    # New schema supports multiple collections; prefer the display-ready rows.
    if isinstance(data.get("occupations_estimated"), list):
        return data["occupations_estimated"]
    if isinstance(data.get("occupations"), list):
        return data["occupations"]
    if isinstance(data.get("soc_groups"), list):
        return data["soc_groups"]

    raise ValueError(
        f"{path} object format missing expected list keys "
        "(occupations_estimated, occupations, soc_groups)"
    )


def read_csv_rows(path: Path) -> List[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def validate_outputs(root: Path, require_scores: bool = True) -> None:
    print("\n[validate] Running output checks")

    occupations_json = root / "occupations.json"
    occupations_csv = root / "occupations.csv"
    scores_json = root / "scores.json"
    site_data_json = root / "site" / "data.json"

    required_files = [occupations_json, occupations_csv, site_data_json]
    if require_scores:
        required_files.append(scores_json)
    missing = [str(p) for p in required_files if not p.exists()]
    if missing:
        raise RuntimeError(f"Missing required output files: {missing}")

    occupations = load_json_array(occupations_json)
    scores = load_json_array(scores_json) if scores_json.exists() else []
    site_data = load_site_rows(site_data_json)
    csv_rows = read_csv_rows(occupations_csv)

    if not csv_rows:
        raise RuntimeError("occupations.csv has no data rows")

    csv_columns = list(csv_rows[0].keys())
    if csv_columns != EXPECTED_CSV_COLUMNS:
        raise RuntimeError(
            "occupations.csv columns do not match expected schema.\n"
            f"Expected: {EXPECTED_CSV_COLUMNS}\n"
            f"Actual:   {csv_columns}"
        )

    csv_count = len(csv_rows)
    site_count = len(site_data)
    score_count = len(scores)
    occ_count = len(occupations)

    if csv_count > occ_count:
        raise RuntimeError(
            f"occupations.csv has more rows ({csv_count}) than "
            f"occupations.json ({occ_count}); source may be corrupt"
        )

    if csv_count != site_count:
        raise RuntimeError(
            f"Row mismatch: occupations.csv={csv_count}, site/data.json={site_count}"
        )

    if require_scores:
        min_score_count = int(csv_count * 0.95)
        if score_count < min_score_count:
            raise RuntimeError(
                f"scores.json coverage too low: {score_count}/{csv_count} "
                f"(need at least {min_score_count})"
            )

    print(f"[validate] occupations.json rows: {occ_count}")
    print(f"[validate] occupations.csv rows: {csv_count}")
    if require_scores:
        print(f"[validate] scores.json rows: {score_count}")
    else:
        print("[validate] scores.json check skipped (score stage skipped)")
    print(f"[validate] site/data.json rows: {site_count}")
    print("[validate] PASS")


def main() -> int:
    args = parse_args()
    root = Path.cwd()

    if args.validate_only:
        try:
            validate_outputs(root, require_scores=True)
            return 0
        except Exception as exc:
            print(f"Validation failed: {exc}", file=sys.stderr)
            return 1

    try:
        selected_stages = stage_range(args.from_stage, args.to_stage)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    score_stage_skipped = False
    for stage in selected_stages:
        if stage == "score" and args.skip_score:
            print("\n[score] Skipped by --skip-score")
            score_stage_skipped = True
            continue
        script = detect_stage_script(stage, root)
        if script is None:
            print(
                f"\n[{stage}] No stage script found. Expected one of: "
                f"{', '.join(STAGE_CANDIDATES[stage])}",
                file=sys.stderr,
            )
            return 3
        try:
            run_stage(stage, script, args.runner, args.dry_run)
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 4

    if not args.skip_validate and not args.dry_run:
        try:
            validate_outputs(root, require_scores=not score_stage_skipped)
        except Exception as exc:
            print(f"Validation failed: {exc}", file=sys.stderr)
            return 5

    print("\nPipeline finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
