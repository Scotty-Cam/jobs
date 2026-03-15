#!/usr/bin/env python3
"""
Build UK occupations.csv with US-compatible schema.

Primary inputs:
- occupations.json
- html/<slug>.html
- pages/<slug>.md

Optional enrichment inputs:
- data/soc_crosswalk.csv (slug,soc_code)
- data/ashe.csv (soc_code,median_pay_annual,median_pay_hourly)
- data/labour_demand.csv (soc_code,year,value)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup


FIELDNAMES = [
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
UK_FULL_TIME_HOURS_PER_YEAR = 1950


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_money_values(text: str) -> List[float]:
    vals = []
    for m in re.findall(r"£\s*([0-9][0-9,]*(?:\.\d+)?)", text):
        try:
            vals.append(float(m.replace(",", "")))
        except ValueError:
            pass
    return vals


def midpoint_or_single(values: List[float]) -> Optional[float]:
    if not values:
        return None
    if len(values) >= 2:
        return (values[0] + values[1]) / 2.0
    return values[0]


def pay_from_text(text: str) -> Tuple[Optional[int], Optional[float]]:
    # Infer unit from local context around each money amount.
    matches = list(re.finditer(r"£\s*([0-9][0-9,]*(?:\.\d+)?)", text))
    if not matches:
        return None, None

    yearly_vals: List[float] = []
    hourly_vals: List[float] = []
    unknown_vals: List[float] = []

    low = text.lower()
    for m in matches:
        raw = m.group(1)
        try:
            val = float(raw.replace(",", ""))
        except ValueError:
            continue
        left = max(0, m.start() - 40)
        right = min(len(low), m.end() + 40)
        ctx = low[left:right]
        if "per hour" in ctx or "an hour" in ctx or "/hour" in ctx:
            hourly_vals.append(val)
        elif "per year" in ctx or "a year" in ctx or "annual" in ctx:
            yearly_vals.append(val)
        else:
            unknown_vals.append(val)

    annual = None
    hourly = None

    y = midpoint_or_single(yearly_vals)
    h = midpoint_or_single(hourly_vals)
    u = midpoint_or_single(unknown_vals)

    if y is not None:
        annual = int(round(y))
        hourly = round(annual / UK_FULL_TIME_HOURS_PER_YEAR, 2)
    elif h is not None:
        hourly = h
        annual = int(round(hourly * UK_FULL_TIME_HOURS_PER_YEAR))
    elif u is not None:
        # Fallback assumption for UK salary ranges is annual.
        annual = int(round(u))
        hourly = round(annual / UK_FULL_TIME_HOURS_PER_YEAR, 2)

    return annual, hourly


def parse_weekly_hours_from_profile_html(soup: BeautifulSoup) -> Optional[float]:
    hours_block = soup.find(id="WorkingHours")
    if not hours_block:
        return None
    text = clean(hours_block.get_text(" "))
    nums = re.findall(r"\b\d+(?:\.\d+)?\b", text)
    if not nums:
        return None
    vals = [float(n) for n in nums]
    if len(vals) >= 2:
        return (vals[0] + vals[1]) / 2.0
    return vals[0]


def pay_from_profile_html(soup: BeautifulSoup) -> Tuple[Optional[int], Optional[float]]:
    salary_block = soup.find(id="Salary")
    if not salary_block:
        return None, None
    text = clean(salary_block.get_text(" "))
    vals = parse_money_values(text)
    base = midpoint_or_single(vals)
    if base is None:
        return None, None
    low = text.lower()
    if "a year" in low or "per year" in low or "annual" in low:
        annual = int(round(base))
        hourly = round(annual / UK_FULL_TIME_HOURS_PER_YEAR, 2)
        return annual, hourly
    if "an hour" in low or "per hour" in low or "/hour" in low:
        hourly = base
        annual = int(round(hourly * UK_FULL_TIME_HOURS_PER_YEAR))
        return annual, hourly
    return int(round(base)), round(base / UK_FULL_TIME_HOURS_PER_YEAR, 2)


def reconcile_pay_pair(
    pay_annual: Optional[int], pay_hourly: Optional[float], weekly_hours: Optional[float]
) -> Tuple[Optional[int], Optional[float]]:
    hours_per_year = int(
        round((weekly_hours * 52.0) if weekly_hours and weekly_hours > 0 else UK_FULL_TIME_HOURS_PER_YEAR)
    )
    hours_per_year = max(520, min(2600, hours_per_year))

    if pay_hourly is None and pay_annual is not None:
        pay_hourly = round(pay_annual / hours_per_year, 2)
    if pay_annual is None and pay_hourly is not None:
        pay_annual = int(round(pay_hourly * hours_per_year))

    if pay_annual is None or pay_hourly is None or pay_hourly <= 0:
        return pay_annual, pay_hourly

    # Hard guard for obvious unit explosions.
    if pay_annual > 1_000_000 and pay_hourly < 200:
        return int(round(pay_hourly * hours_per_year)), pay_hourly

    implied_weekly_hours = (pay_annual / pay_hourly) / 52.0
    if weekly_hours and weekly_hours > 0:
        # Only correct when annual/hourly are clearly incompatible with profile working-hours.
        if abs(implied_weekly_hours - weekly_hours) > 12:
            pay_annual = int(round(pay_hourly * weekly_hours * 52.0))
    else:
        # Without working-hours evidence, only normalize extreme implausible pairings.
        if implied_weekly_hours < 10 or implied_weekly_hours > 60:
            pay_annual = int(round(pay_hourly * UK_FULL_TIME_HOURS_PER_YEAR))

    return pay_annual, pay_hourly


def _has_any(text: str, patterns: List[str]) -> bool:
    for pat in patterns:
        if re.search(pat, text):
            return True
    return False


def education_bucket(text: str) -> str:
    t = text.lower()
    # Prioritize highest qualification indicators first to avoid downgrading
    # degree-required roles that also mention apprenticeships/college routes.
    # Use word-boundary patterns for short abbreviations to avoid false positives.
    if _has_any(
        t,
        [
            r"doctorate",
            r"\bphd\b",
            r"\bdphil\b",
            r"\bmd\b",
            r"medical degree",
            r"veterinary degree",
            r"professional qualification",
            r"registration with",
            r"\bchartered\b",
            r"\bsolicitor\b",
            r"\bbarrister\b",
        ],
    ):
        return "Doctoral or professional degree"
    if _has_any(
        t,
        [
            r"\bmaster",
            r"\bmsc\b",
            r"\bma\b",
            r"\bmres\b",
            r"postgraduate diploma",
            r"postgraduate qualification",
        ],
    ):
        return "Master's degree"
    if _has_any(
        t,
        [
            r"\bbachelor",
            r"\bbsc\b",
            r"\bba\b",
            r"\bbed\b",
            r"undergraduate degree",
            r"degree in\b",
            r"degree apprenticeship",
            r"\bgraduate\b",
        ],
    ):
        return "Bachelor's degree"

    if _has_any(t, [r"no qualifications", r"no formal qualifications"]):
        return "No formal educational credential"
    if _has_any(t, [r"\bgcse\b", r"\ba level", r"high school"]):
        return "High school diploma or equivalent"
    if _has_any(t, [r"\bhnd\b", r"\bhnc\b", r"foundation degree"]):
        return "Associate's degree"
    if _has_any(t, [r"\bapprenticeship\b", r"college course", r"\blevel 3\b", r"\blevel 4\b", r"\blevel 5\b"]):
        return "Postsecondary nondegree award"
    return "See How to Become One"


def outlook_desc(pct: Optional[int]) -> str:
    if pct is None:
        return ""
    if pct >= 20:
        return "Much faster than average"
    if pct >= 10:
        return "Faster than average"
    if pct >= 3:
        return "Average"
    if pct >= 0:
        return "Little or no change"
    if pct >= -9:
        return "Decline"
    return "Strong decline"


def parse_soc_from_html(html_text: str) -> Optional[str]:
    # Only trust explicit SOC mentions near a 4-digit code.
    soup = BeautifulSoup(html_text, "html.parser")
    text = clean(soup.get_text(" "))
    patterns = [
        r"\bsoc(?:\s+code)?\s*[:\-]?\s*([1-9][0-9]{3})\b",
        r"\bstandard occupational classification(?:\s*\(soc\))?\s*[:\-]?\s*([1-9][0-9]{3})\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def load_crosswalk(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    out = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = clean(row.get("slug", ""))
            soc = clean(row.get("soc_code", ""))
            if slug and soc:
                out[slug] = soc
    return out


def load_ashe(path: Path) -> Dict[str, Tuple[Optional[int], Optional[float]]]:
    if not path.exists():
        return {}
    out: Dict[str, Tuple[Optional[int], Optional[float]]] = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            soc = clean(row.get("soc_code", ""))
            if not soc:
                continue
            annual = clean(row.get("median_pay_annual", ""))
            hourly = clean(row.get("median_pay_hourly", ""))
            a_val = int(float(annual)) if annual else None
            h_val = round(float(hourly), 2) if hourly else None
            out[soc] = (a_val, h_val)
    return out


def load_labour(path: Path) -> Dict[str, Dict[int, float]]:
    if not path.exists():
        return {}
    out: Dict[str, Dict[int, float]] = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            soc = clean(row.get("soc_code", ""))
            year = clean(row.get("year", ""))
            val = clean(row.get("value", ""))
            if not soc or not year or not val:
                continue
            try:
                y = int(year)
                v = float(val)
            except ValueError:
                continue
            out.setdefault(soc, {})
            out[soc][y] = out[soc].get(y, 0.0) + v
    return out


def projection_from_series(series: Dict[int, float]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    # Returns (jobs_2024, projected_2034, outlook_pct)
    if not series:
        return None, None, None
    base = series.get(2024)
    if base is None or base <= 0:
        return None, None, None

    fit_years = [y for y in range(2019, 2026) if y in series and series[y] > 0]
    if len(fit_years) < 2:
        # No trend fit: flat projection.
        proj = int(round(base))
        return int(round(base)), proj, 0

    x = fit_years
    y = [math.log1p(series[yy]) for yy in x]
    x_bar = sum(x) / len(x)
    y_bar = sum(y) / len(y)
    denom = sum((xx - x_bar) ** 2 for xx in x)
    if denom == 0:
        g = 0.0
    else:
        m = sum((xx - x_bar) * (yy - y_bar) for xx, yy in zip(x, y)) / denom
        g = math.exp(m) - 1.0
    g = max(-0.08, min(0.08, g))

    proj = int(round(base * ((1 + g) ** 10)))
    pct = int(round(((proj / base) - 1) * 100))
    return int(round(base)), proj, pct


def extract_profile_fallbacks(
    html_text: str, md_text: str
) -> Tuple[Optional[int], Optional[float], Optional[float], str, str, str]:
    soup = BeautifulSoup(html_text, "html.parser")
    text = clean(soup.get_text(" "))
    md = md_text or ""
    merged = f"{text}\n{md}"

    weekly_hours = parse_weekly_hours_from_profile_html(soup)
    pay_annual, pay_hourly = pay_from_profile_html(soup)
    if pay_annual is None and pay_hourly is None:
        pay_annual, pay_hourly = pay_from_text(merged)

    education = education_bucket(merged)
    work_exp = ""
    training = ""

    merged_low = merged.lower()
    if "work experience" in merged_low:
        work_exp = "Some related experience"
    if any(t in merged_low for t in ["apprenticeship", "on the job", "training"]):
        training = "On-the-job training"
    return pay_annual, pay_hourly, weekly_hours, education, work_exp, training


def fmt_int(value: Optional[int]) -> str:
    return str(value) if value is not None else ""


def fmt_hourly(value: Optional[float]) -> str:
    return f"{value:.2f}" if value is not None else ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Build UK occupations.csv")
    parser.add_argument("--crosswalk", default="data/soc_crosswalk.csv")
    parser.add_argument("--ashe", default="data/ashe.csv")
    parser.add_argument("--labour", default="data/labour_demand.csv")
    args = parser.parse_args()

    root = Path(".")
    occupations = json.loads((root / "occupations.json").read_text())
    crosswalk = load_crosswalk(root / args.crosswalk)
    ashe = load_ashe(root / args.ashe)
    labour = load_labour(root / args.labour)
    if not crosswalk:
        print(f"WARN: no SOC crosswalk loaded from {args.crosswalk}; SOC coverage may be low.")
    if not ashe:
        print(f"WARN: no ASHE pay enrichment loaded from {args.ashe}; using profile-text fallback pay only.")
    if not labour:
        print(
            f"WARN: no labour-demand enrichment loaded from {args.labour}; "
            "jobs/outlook proxy fields will be blank."
        )

    rows = []
    missing_html = 0
    mapped_soc = 0

    for occ in occupations:
        slug = occ["slug"]
        html_path = root / "html" / f"{slug}.html"
        md_path = root / "pages" / f"{slug}.md"
        if not html_path.exists():
            missing_html += 1
            continue

        html_text = html_path.read_text()
        md_text = md_path.read_text() if md_path.exists() else ""

        soc = crosswalk.get(slug) or parse_soc_from_html(html_text) or ""
        if soc:
            mapped_soc += 1

        (
            pay_annual_fb,
            pay_hourly_fb,
            weekly_hours,
            edu,
            work_exp,
            training,
        ) = extract_profile_fallbacks(html_text, md_text)
        pay_annual_ashe, pay_hourly_ashe = ashe.get(soc, (None, None)) if soc else (None, None)

        # Build pay values with source precedence:
        # 1) ASHE direct field
        # 2) derived from paired ASHE field
        # 3) profile fallback
        pay_hourly = pay_hourly_ashe
        if pay_hourly is None:
            pay_hourly = pay_hourly_fb

        pay_annual = pay_annual_ashe
        if pay_annual is None:
            pay_annual = pay_annual_fb

        pay_annual, pay_hourly = reconcile_pay_pair(pay_annual, pay_hourly, weekly_hours)

        jobs_2024 = proj_2034 = out_pct = None
        if soc and soc in labour:
            jobs_2024, proj_2034, out_pct = projection_from_series(labour[soc])

        change = (proj_2034 - jobs_2024) if (proj_2034 is not None and jobs_2024 is not None) else None

        row = {
            "title": occ["title"],
            "category": occ["category"],
            "slug": slug,
            "soc_code": soc,
            "median_pay_annual": fmt_int(pay_annual),
            "median_pay_hourly": fmt_hourly(pay_hourly),
            "entry_education": edu,
            "work_experience": work_exp,
            "training": training,
            "num_jobs_2024": fmt_int(jobs_2024),
            "projected_employment_2034": fmt_int(proj_2034),
            "outlook_pct": fmt_int(out_pct),
            "outlook_desc": outlook_desc(out_pct),
            "employment_change": fmt_int(change),
            "url": occ["url"],
        }
        rows.append(row)

    with open("occupations.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote occupations.csv with {len(rows)} rows")
    print(f"Missing html rows skipped: {missing_html}")
    print(f"SOC mapped rows: {mapped_soc}/{len(rows) if rows else 0}")
    print(f"ASHE source loaded: {len(ashe)} SOC rows")
    print(f"Labour source loaded: {len(labour)} SOC rows")


if __name__ == "__main__":
    main()
