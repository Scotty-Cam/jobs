#!/usr/bin/env python3
"""
Use an LLM (via OpenRouter) to improve SOC mappings in data/soc_crosswalk.csv.

The script is constrained:
- It generates a shortlist of SOC candidates from official SOC labels.
- The LLM must select from those candidates (or return "no_match").
- Results are written incrementally so runs can resume safely.

Inputs:
- occupations.json
- data/soc_crosswalk.csv
- data/labour_demand_source.xlsx (for SOC2020 code -> label dictionary)

Output:
- data/soc_crosswalk.csv (updated with llm_* metadata + soc_code updates)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Tuple

import httpx
import openpyxl
from dotenv import load_dotenv


load_dotenv()

API_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "google/gemini-3-flash-preview"
DEFAULT_TOP_K = 8
DEFAULT_DELAY = 0.2
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF = 1.5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LLM SOC mapping adjudication")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--crosswalk", default="data/soc_crosswalk.csv")
    parser.add_argument("--occupations", default="occupations.json")
    parser.add_argument("--labour-source", default="data/labour_demand_source.xlsx")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-process rows even if llm_soc_code already exists",
    )
    parser.add_argument(
        "--review-threshold",
        type=float,
        default=0.80,
        help="Mark llm_review_required=yes when confidence is below this value",
    )
    return parser.parse_args()


def norm(text: str) -> str:
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def score_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def load_soc_labels_from_labour_xlsx(path: Path) -> Dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing labour source workbook: {path}")
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb["Table 3"]
    labels: Dict[str, str] = {}
    for row in ws.iter_rows(min_row=6, values_only=True):
        region, code, label = row[0], row[1], row[2]
        if region != "Total UK" or code is None or label is None:
            continue
        soc = str(code).strip()
        if not re.fullmatch(r"\d{4}", soc):
            continue
        if soc not in labels:
            labels[soc] = str(label).strip()
    return labels


def top_candidates(title: str, labels: Dict[str, str], k: int) -> List[Tuple[str, str, float]]:
    ntitle = norm(title)
    scored = []
    for soc, label in labels.items():
        s = score_similarity(ntitle, norm(label))
        scored.append((soc, label, s))
    scored.sort(key=lambda x: x[2], reverse=True)
    return scored[:k]


def parse_json_content(content: str) -> Dict:
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1]
        if content.endswith("```"):
            content = content[:-3]
    return json.loads(content.strip())


def llm_pick_soc(
    client: httpx.Client,
    model: str,
    title: str,
    current_soc: str,
    candidates: List[Tuple[str, str, float]],
) -> Dict:
    candidate_lines = [
        f"- {code}: {label} (sim={sim:.3f})" for code, label, sim in candidates
    ]
    allowed_codes = [code for code, _, _ in candidates]
    sys_prompt = (
        "You are mapping UK job titles to UK SOC2020 4-digit codes.\n"
        "You MUST choose exactly one SOC code from the provided candidates, or 'no_match'.\n"
        "Do not invent codes.\n"
        "Return JSON only with keys: soc_code, confidence, reason.\n"
        "confidence must be a number in [0,1]."
    )
    user_prompt = (
        f"Job title: {title}\n"
        f"Current SOC (may be wrong): {current_soc or 'blank'}\n"
        f"Allowed SOC candidates:\n" + "\n".join(candidate_lines) + "\n\n"
        "Return JSON:\n"
        '{ "soc_code": "<one allowed code or no_match>", "confidence": 0.0, "reason": "..." }'
    )

    r = client.post(
        API_URL,
        headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        },
        timeout=90,
    )
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"]
    parsed = parse_json_content(content)

    soc_code = str(parsed.get("soc_code", "")).strip()
    conf = float(parsed.get("confidence", 0.0))
    reason = str(parsed.get("reason", "")).strip()

    if soc_code != "no_match" and soc_code not in allowed_codes:
        raise ValueError(
            f"Model returned non-candidate SOC code '{soc_code}'. Allowed: {allowed_codes}"
        )
    conf = max(0.0, min(1.0, conf))
    return {"soc_code": soc_code, "confidence": conf, "reason": reason}


def llm_pick_soc_with_retry(
    client: httpx.Client,
    model: str,
    title: str,
    current_soc: str,
    candidates: List[Tuple[str, str, float]],
    retries: int,
    backoff: float,
) -> Dict:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return llm_pick_soc(client, model, title, current_soc, candidates)
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            time.sleep(backoff * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def main() -> None:
    args = parse_args()
    if "OPENROUTER_API_KEY" not in os.environ:
        raise RuntimeError(
            "OPENROUTER_API_KEY is required. Add to .env or shell environment."
        )

    root = Path(".")
    occ_path = root / args.occupations
    crosswalk_path = root / args.crosswalk
    labour_source_path = root / args.labour_source

    occupations = json.loads(occ_path.read_text())
    occ_by_slug = {o["slug"]: o for o in occupations}

    with crosswalk_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    extra_fields = [
        "llm_soc_code",
        "llm_confidence",
        "llm_reason",
        "llm_model",
        "llm_review_required",
        "llm_candidates",
    ]
    for field in extra_fields:
        if field not in fieldnames:
            fieldnames.append(field)

    soc_labels = load_soc_labels_from_labour_xlsx(labour_source_path)

    subset = rows[args.start : args.end]
    client = httpx.Client()
    processed = 0
    skipped = 0
    errors = 0

    try:
        for i, row in enumerate(subset, start=1):
            slug = row.get("slug", "")
            occ = occ_by_slug.get(slug, {"title": row.get("title", slug)})
            title = occ.get("title", slug)

            if row.get("llm_soc_code") and not args.force:
                skipped += 1
                continue

            candidates = top_candidates(title, soc_labels, args.top_k)
            if not candidates:
                row["llm_soc_code"] = ""
                row["llm_confidence"] = "0"
                row["llm_reason"] = "No SOC candidates available"
                row["llm_model"] = args.model
                row["llm_review_required"] = "yes"
                row["llm_candidates"] = ""
                errors += 1
                continue

            current_soc = row.get("soc_code", "").strip()
            try:
                decision = llm_pick_soc_with_retry(
                    client=client,
                    model=args.model,
                    title=title,
                    current_soc=current_soc,
                    candidates=candidates,
                    retries=args.retries,
                    backoff=args.backoff,
                )
                llm_soc = decision["soc_code"]
                conf = decision["confidence"]
                reason = decision["reason"]

                if llm_soc != "no_match":
                    row["soc_code"] = llm_soc
                    row["llm_soc_code"] = llm_soc
                else:
                    row["llm_soc_code"] = ""

                row["llm_confidence"] = f"{conf:.3f}"
                row["llm_reason"] = reason
                row["llm_model"] = args.model
                row["llm_review_required"] = (
                    "yes" if conf < args.review_threshold else "no"
                )
                row["llm_candidates"] = "|".join([c[0] for c in candidates])
                processed += 1
                print(
                    f"[{i}/{len(subset)}] {title} -> {row.get('soc_code','')} "
                    f"(conf={conf:.2f}, review={row['llm_review_required']})"
                )
            except Exception as exc:
                row["llm_confidence"] = "0"
                row["llm_reason"] = f"ERROR: {exc}"
                row["llm_model"] = args.model
                row["llm_review_required"] = "yes"
                row["llm_candidates"] = "|".join([c[0] for c in candidates])
                errors += 1
                print(f"[{i}/{len(subset)}] {title} -> ERROR: {exc}")

            with crosswalk_path.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            if i < len(subset):
                time.sleep(args.delay)
    finally:
        client.close()

    print(
        f"\nDone. processed={processed}, skipped={skipped}, errors={errors}, "
        f"rows_total={len(rows)}"
    )


if __name__ == "__main__":
    main()
