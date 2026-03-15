#!/usr/bin/env python3
"""
Build site/data.json from occupations.csv + scores.json.
Schema matches the US project.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def to_int(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def to_float(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def pick_mode(values: List[str]) -> str:
    cleaned = [v for v in values if v]
    if not cleaned:
        return ""
    return Counter(cleaned).most_common(1)[0][0]


def weighted_avg(values: List[Tuple[float, int]]) -> Optional[float]:
    if not values:
        return None
    denom = sum(weight for _, weight in values)
    if denom <= 0:
        return None
    return sum(val * weight for val, weight in values) / denom


def load_crosswalk_signals(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    out: Dict[str, dict] = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = (row.get("slug") or "").strip()
            if not slug:
                continue
            out[slug] = {
                "llm_confidence": to_float(row.get("llm_confidence", "")),
                "api_score": to_float(row.get("api_score", "")),
            }
    return out


def title_weight(slug: str, signals: Dict[str, dict]) -> float:
    sig = signals.get(slug, {})
    llm = sig.get("llm_confidence")
    api = sig.get("api_score")
    w = 1.0
    if llm is not None:
        w += max(0.0, min(1.0, llm))
    if api is not None:
        w += max(0.0, min(100.0, api)) / 100.0
    return w


def split_integer_total(total: int, weights: List[float]) -> List[int]:
    if total <= 0 or not weights:
        return [0 for _ in weights]
    s = sum(weights)
    if s <= 0:
        even = total // len(weights)
        rem = total - even * len(weights)
        out = [even for _ in weights]
        for i in range(rem):
            out[i] += 1
        return out
    raw = [total * (w / s) for w in weights]
    floors = [int(v) for v in raw]
    rem = total - sum(floors)
    fracs = sorted(range(len(raw)), key=lambda i: (raw[i] - floors[i]), reverse=True)
    for i in range(rem):
        floors[fracs[i % len(fracs)]] += 1
    return floors


def infer_family(title: str, soc_code: str, category: str) -> str:
    t = (title or "").lower()
    keyword_families: List[Tuple[List[str], str]] = [
        ([r"\bnurse", r"\bdoctor\b", r"\btherap", r"\bmidwife", r"\bparamedic", r"\bdent", r"\bcare\b", r"social worker"], "Healthcare & Care"),
        ([r"\bteacher\b", r"\blecturer\b", r"\btutor\b", r"\beducation"], "Education"),
        ([r"\bengineer", r"\bdeveloper\b", r"\bprogrammer\b", r"\bdata\b", r"\btechnician\b", r"\bit\b", r"\bsoftware\b"], "STEM & Technology"),
        ([r"\bdriver\b", r"\bpilot\b", r"\btransport", r"\blogistic", r"\bdelivery\b", r"\brail", r"\bbus\b", r"\btaxi\b"], "Transport & Logistics"),
        ([r"\bchef\b", r"\bcook\b", r"\bwaiter", r"\bhospitality", r"\bhotel\b", r"\bbar\b", r"\brestaurant"], "Hospitality & Food"),
        ([r"\bsales\b", r"\bretail\b", r"\bcustomer\b", r"\bshop\b", r"account manager", r"\bmarketing\b", r"\badvertising"], "Sales & Customer"),
        ([r"\bconstruction\b", r"\belectrician\b", r"\bplumber\b", r"\bcarpenter\b", r"\bbrick", r"\bwelder\b", r"\bbuilder\b"], "Skilled Trades & Construction"),
        ([r"\bmanager\b", r"\bdirector\b", r"\bexecutive\b", r"\badministrat", r"\bsecretary\b", r"\bhr\b", r"\boffice\b"], "Management & Admin"),
        ([r"\bpolice\b", r"\bfire\b", r"\bsecurity\b", r"\bguard\b", r"\barmed\b", r"\bprison\b"], "Public Services & Safety"),
        ([r"\bartist\b", r"\bdesigner\b", r"\bwriter\b", r"\bjournalist\b", r"\bmusician\b", r"\bactor\b", r"\bmedia\b"], "Creative & Media"),
    ]
    for patterns, fam in keyword_families:
        if any(re.search(p, t) for p in patterns):
            return fam

    major = soc_code[:1] if soc_code else ""
    major_map = {
        "1": "Management & Admin",
        "2": "Professional Services",
        "3": "Associate Professional",
        "4": "Admin & Secretarial",
        "5": "Skilled Trades & Construction",
        "6": "Care, Leisure & Other Services",
        "7": "Sales & Customer",
        "8": "Process, Plant & Machine",
        "9": "Elementary & Operatives",
    }
    if major in major_map:
        return major_map[major]
    if category:
        return category.replace("-", " ").title()
    return "Other"


def main() -> None:
    root = Path(".")
    csv_path = root / "occupations.csv"
    scores_path = root / "scores.json"
    site_dir = root / "site"
    out_path = site_dir / "data.json"
    crosswalk_path = root / "data" / "soc_crosswalk.csv"
    site_dir.mkdir(parents=True, exist_ok=True)

    scores: Dict[str, dict] = {}
    if scores_path.exists():
        for row in json.loads(scores_path.read_text()):
            scores[row["slug"]] = row
    signals = load_crosswalk_signals(crosswalk_path)

    with csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))

    occupations = []
    for row in rows:
        slug = row["slug"]
        sc = scores.get(slug, {})
        occupations.append(
            {
                "title": row["title"],
                "slug": slug,
                "category": row["category"],
                "soc_code": row.get("soc_code", ""),
                "pay": to_int(row.get("median_pay_annual", "")),
                "pay_hourly": to_float(row.get("median_pay_hourly", "")),
                "jobs": to_int(row.get("num_jobs_2024", "")),
                "outlook": to_int(row.get("outlook_pct", "")),
                "outlook_desc": row.get("outlook_desc", ""),
                "education": row.get("entry_education", ""),
                "exposure": sc.get("exposure"),
                "exposure_rationale": sc.get("rationale"),
                "url": row.get("url", ""),
            }
        )

    by_soc = defaultdict(list)
    for occ in occupations:
        soc_key = occ.get("soc_code") or f"unmapped:{occ['slug']}"
        by_soc[soc_key].append(occ)

    occupations_estimated = []
    soc_groups = []
    for soc_key, members in by_soc.items():
        members = sorted(members, key=lambda x: x["title"])
        jobs_vals = [m["jobs"] for m in members if m.get("jobs") is not None]
        group_jobs = max(jobs_vals) if jobs_vals else None
        if group_jobs is not None:
            weights = [title_weight(m["slug"], signals) for m in members]
            split_jobs = split_integer_total(group_jobs, weights)
        else:
            split_jobs = [None for _ in members]

        pay_pairs = []
        exposure_pairs = []
        for idx, m in enumerate(members):
            est_jobs = split_jobs[idx]
            weight = est_jobs or 0
            if m.get("pay") is not None and weight > 0:
                pay_pairs.append((float(m["pay"]), weight))
            if m.get("exposure") is not None and weight > 0:
                exposure_pairs.append((float(m["exposure"]), weight))

        group_pay = weighted_avg(pay_pairs)
        group_exposure = weighted_avg(exposure_pairs)

        rep = max(members, key=lambda x: x.get("jobs") or 0)
        if len(members) == 1:
            display_title = rep["title"]
        else:
            display_title = f"{rep['title']} +{len(members) - 1} related roles"
        roles = [
            {
                "title": m["title"],
                "slug": m["slug"],
                "url": m["url"],
                "exposure": m.get("exposure"),
                "jobs_est": split_jobs[idx],
            }
            for idx, m in enumerate(members)
        ]

        for idx, m in enumerate(members):
            est_jobs = split_jobs[idx]
            occupations_estimated.append(
                {
                    **m,
                    "jobs": est_jobs,
                    "jobs_soc_total": group_jobs,
                    "jobs_estimated": len(members) > 1 and est_jobs is not None,
                    "family": infer_family(m.get("title", ""), m.get("soc_code", ""), m.get("category", "")),
                }
            )

        soc_groups.append(
            {
                "title": display_title,
                "soc_code": "" if soc_key.startswith("unmapped:") else soc_key,
                "category": rep.get("category", ""),
                "role_count": len(members),
                "roles": roles,
                "pay": int(round(group_pay)) if group_pay is not None else None,
                "jobs": group_jobs,
                "outlook": rep.get("outlook"),
                "outlook_desc": rep.get("outlook_desc", ""),
                "education": pick_mode([m.get("education", "") for m in members]),
                "exposure": int(round(group_exposure)) if group_exposure is not None else None,
                "exposure_rationale": "",
                "url": "",
            }
        )

    soc_groups.sort(key=lambda x: ((x.get("jobs") or 0), x["title"]), reverse=True)
    occupations_estimated.sort(key=lambda x: ((x.get("jobs") or 0), x["title"]), reverse=True)

    out_payload = {
        "meta": {
            "view_unit": "occupation",
            "jobs_level": "title_estimated_from_soc",
            "note": "covered SOC groups, not whole UK labour market",
        },
        "occupations": occupations,
        "occupations_estimated": occupations_estimated,
        "soc_groups": soc_groups,
    }
    out_path.write_text(json.dumps(out_payload))
    print(f"Wrote {out_path} with {len(occupations)} occupations and {len(soc_groups)} SOC groups")


if __name__ == "__main__":
    main()
