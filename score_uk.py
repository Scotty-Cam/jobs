#!/usr/bin/env python3
"""
Score each UK occupation's AI exposure using OpenRouter.

Reads markdown pages from pages/ and occupations from occupations.json.
Writes scores.json incrementally so the run can resume.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx
from dotenv import load_dotenv


load_dotenv()

DEFAULT_MODEL = "google/gemini-3-flash-preview"
OUTPUT_FILE = "scores.json"
API_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF = 1.5

SYSTEM_PROMPT = """\
You are an expert labour-market analyst evaluating how exposed UK occupations are to AI.
You will receive a job profile text for a UK occupation.

Rate overall AI Exposure from 0 to 10 where:
- 0 means almost no exposure to AI-driven change
- 10 means maximum exposure to AI-driven restructuring

Consider:
1) direct automation risk,
2) augmentation/productivity effects,
3) whether work is mostly digital vs physical/in-person.

Return ONLY valid JSON:
{
  "exposure": <integer 0-10>,
  "rationale": "<2-3 concise sentences>"
}
"""


def parse_json_content(content: Optional[str]) -> Dict:
    if content is None:
        raise ValueError("Model returned null content")
    content = content.strip()
    if not content:
        raise ValueError("Model returned empty content")
    if content.startswith("```"):
        content = content.split("\n", 1)[1]
        if content.endswith("```"):
            content = content[:-3]
    return json.loads(content.strip())


def score_text(client: httpx.Client, model: str, text: str) -> Dict:
    r = client.post(
        API_URL,
        headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
            "temperature": 0.2,
        },
        timeout=90,
    )
    r.raise_for_status()
    payload = r.json()
    choice = payload["choices"][0]
    message = choice.get("message", {})
    content = message.get("content")
    try:
        return parse_json_content(content)
    except Exception as exc:
        raise ValueError(
            "Unable to parse model response content. "
            f"finish_reason={choice.get('finish_reason')}, "
            f"message_keys={list(message.keys())}"
        ) from exc


def score_text_with_retry(
    client: httpx.Client, model: str, text: str, retries: int, backoff: float
) -> Dict:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return score_text(client, model, text)
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            time.sleep(backoff * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("All retry attempts failed")


def main() -> None:
    parser = argparse.ArgumentParser(description="Score UK occupations")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if "OPENROUTER_API_KEY" not in os.environ:
        raise RuntimeError(
            "OPENROUTER_API_KEY is required. Add it to environment or .env:\n"
            "OPENROUTER_API_KEY=your_key_here"
        )

    occupations = json.loads(Path("occupations.json").read_text())
    subset = occupations[args.start : args.end]

    scores_by_slug: Dict[str, Dict] = {}
    if Path(OUTPUT_FILE).exists() and not args.force:
        existing = json.loads(Path(OUTPUT_FILE).read_text())
        for row in existing:
            scores_by_slug[row["slug"]] = row

    client = httpx.Client()
    errors: List[str] = []

    print(f"Scoring {len(subset)} occupations with {args.model}")
    print(f"Cached scores: {len(scores_by_slug)}")

    try:
        for i, occ in enumerate(subset, start=1):
            slug = occ["slug"]
            if slug in scores_by_slug:
                continue

            md_path = Path("pages") / f"{slug}.md"
            if not md_path.exists():
                print(f"[{i}/{len(subset)}] SKIP {slug} (no markdown)")
                continue

            text = md_path.read_text()
            print(f"[{i}/{len(subset)}] {occ['title']} ... ", end="", flush=True)
            wrote_new = False
            try:
                result = score_text_with_retry(
                    client, args.model, text, retries=args.retries, backoff=args.backoff
                )
                if "exposure" not in result:
                    raise ValueError(f"Missing 'exposure' key in response: {result}")
                exposure = int(result["exposure"])
                exposure = max(0, min(10, exposure))
                rationale = str(result.get("rationale") or "").strip()
                scores_by_slug[slug] = {
                    "slug": slug,
                    "title": occ["title"],
                    "exposure": exposure,
                    "rationale": rationale,
                }
                wrote_new = True
                print(f"exposure={exposure}")
            except Exception as exc:
                print(f"ERROR: {exc}")
                errors.append(slug)

            if wrote_new:
                Path(OUTPUT_FILE).write_text(
                    json.dumps(list(scores_by_slug.values()), indent=2)
                )
            if i < len(subset):
                time.sleep(args.delay)
    finally:
        client.close()

    print(f"\nDone. scores={len(scores_by_slug)}, errors={len(errors)}")
    if errors:
        print(f"Error slugs: {errors}")


if __name__ == "__main__":
    main()
