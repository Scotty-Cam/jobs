# AI Exposure of the UK Job Market

Analyzing how susceptible occupations in the UK economy are to AI and automation, using UK occupation profiles and labour market data.

## What this project does

This project builds a UK equivalent of the AI exposure analysis pipeline and website.

It:

- collects occupation profiles from National Careers Service
- maps each profile to SOC2020 occupation codes
- enriches occupations with UK earnings and labour demand data
- scores AI exposure per occupation using an LLM
- produces a compact website dataset for interactive visualization

The output schema is intentionally compatible with the original US project structure so the same frontend pattern can be reused.

## Data pipeline

1. **Scrape occupation profiles** (`scrape_uk.py`)  
   Crawls National Careers Service career pages and caches raw HTML in `html/`.  
   Builds `occupations.json` with title, URL, category, and slug.

2. **Parse occupation pages to Markdown** (`process_uk.py`)  
   Converts cached HTML pages into normalized Markdown files in `pages/`.

3. **Build UK occupation table** (`make_csv_uk.py`)  
   Produces `occupations.csv` with a US-compatible schema:
   - title, category, slug, SOC code
   - pay fields
   - education/training/work experience
   - jobs and outlook proxy fields

4. **Score AI exposure** (`score_uk.py`)  
   Sends occupation Markdown to an LLM via OpenRouter and writes:
   - `exposure` (0 to 10)
   - `rationale`  
   Results are checkpointed in `scores.json`.

5. **Build website data** (`build_site_data_uk.py`)  
   Merges `occupations.csv`, `scores.json`, and SOC mapping confidence metadata into `site/data.json`.
   The output now includes:
   - `occupations`: raw title-level rows
   - `occupations_estimated`: title-level rows with estimated jobs for shared SOC groups
   - `soc_groups`: SOC-group rows for audit/transparency
   - `meta`: notes describing estimation logic used by the frontend

6. **Orchestrate end to end run** (`run_uk_pipeline.py`)  
   Runs stages in sequence with resumable checkpoints and validation.

## UK methodology

### Occupation source

Primary occupation profiles come from National Careers Service:

- Explore careers: [https://nationalcareers.service.gov.uk/explore-careers](https://nationalcareers.service.gov.uk/explore-careers)
- All careers: [https://nationalcareers.service.gov.uk/explore-careers/all-careers](https://nationalcareers.service.gov.uk/explore-careers/all-careers)

### SOC mapping

Occupation titles are mapped to SOC2020 codes using a hybrid approach:

- automated matching and candidate ranking
- SOC API hints and alias titles
- manual review for ambiguous rows

The reviewed mapping is stored in `data/soc_crosswalk.csv`.

### Earnings enrichment

Pay fields are sourced from ONS ASHE Table 14:

- [https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/occupation4digitsoc2010ashetable14](https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/occupation4digitsoc2010ashetable14)

### Jobs and outlook proxy

Jobs and outlook proxy metrics are derived from ONS SOC2020 labour demand tables:

- [https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/datasets/labourdemandvolumesbystandardoccupationclassificationsoc2020uk](https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/datasets/labourdemandvolumesbystandardoccupationclassificationsoc2020uk)

When multiple NCS job titles map to the same SOC code, title-level jobs are estimated by weighted splitting of the SOC total. Weights use confidence signals from `data/soc_crosswalk.csv` (`llm_confidence`, `api_score`) and preserve exact SOC totals when summed.

### LLM scoring

Each occupation is scored on one axis, **AI Exposure (0 to 10)**:

- 0 to 1: minimal exposure
- 2 to 3: low exposure
- 4 to 5: moderate exposure
- 6 to 7: high exposure
- 8 to 9: very high exposure
- 10: maximum exposure

The score considers direct automation risk and indirect productivity effects from AI.

## Key files

- `occupations.json` - master occupation list with URLs and slugs
- `occupations.csv` - structured UK occupation table in US-compatible schema
- `scores.json` - AI exposure scores and rationales
- `html/` - raw cached profile HTML
- `pages/` - parsed Markdown occupation profiles
- `site/data.json` - website dataset with `occupations`, `occupations_estimated`, `soc_groups`, and `meta`
- `data/soc_crosswalk.csv` - reviewed SOC mapping table
- `data/manual_review*.csv` - review queues and decisions

## Setup

Requires Python 3.10+.

Install dependencies (using uv):

```bash
uv sync
```

Or with pip:

```bash
python3 -m pip install httpx beautifulsoup4 python-dotenv openpyxl rapidfuzz
```

Create a local `.env` file for scoring:

```bash
OPENROUTER_API_KEY=your_key_here
```

Do not commit `.env` to source control.

## Usage

### 1) Full pipeline

```bash
python3 run_uk_pipeline.py --runner "python3"
```

### 2) Data build only (skip scoring)

```bash
python3 run_uk_pipeline.py --skip-score --runner "python3"
```

### 3) Run scoring after data build

```bash
python3 run_uk_pipeline.py --from-stage score --runner "python3"
```

### 4) Validate outputs only

```bash
python3 run_uk_pipeline.py --validate-only
```

### 5) Serve site locally

```bash
cd site
python3 -m http.server 8000
```

Then open `http://localhost:8000`.

## Optional enrichment inputs

`make_csv_uk.py` supports these enrichment files:

- `data/soc_crosswalk.csv` (`slug,soc_code` plus review metadata)
- `data/ashe.csv` (`soc_code,median_pay_annual,median_pay_hourly`)
- `data/labour_demand.csv` (`soc_code,year,value`)

Without these files, pay/jobs/outlook quality is reduced.

## Known limitations

- **Jobs and outlook are proxy-derived:** `num_jobs_2024`, `projected_employment_2034`, and related outlook fields are based on labour demand series methodology, not direct official long-run occupation projections.
- **Some title-level jobs are estimated:** when a SOC code covers multiple titles, individual title job counts are modelled from SOC totals using weighted splits.
- **SOC mapping quality depends on review:** automated and API-assisted mapping is used, but edge cases still require manual judgement for best semantic fit.
- **NCS profile structure may change:** scraper and parser logic depends on current page structure and may need updates if the site markup changes.
- **LLM scoring is model-dependent:** exposure scores and rationales are probabilistic outputs that can vary slightly by model and prompt configuration.
- **Data vintages differ across sources:** occupation profiles, earnings tables, and labour demand releases may not align to the exact same reference date.

## Frontend display notes

- Treemap groups occupations into custom logical families (for example Healthcare, Transport, Management) to improve readability.
- Tile job counts are title-level values; for shared SOC mappings these are weighted estimates.
- Tooltip shows estimated-job context where applicable (including SOC total jobs for transparency).

## Output validation expectations

The pipeline validates:

- required artifacts exist
- CSV schema matches expected structure
- row counts match between `occupations.csv` and `site/data.json` (`occupations` / `occupations_estimated` collections)
- score coverage checks when scoring is included

## Credits and reference project

This project is a UK adaptation inspired by:

- US reference repo: [https://github.com/Scotty-Cam/AIExposureUS](https://github.com/Scotty-Cam/AIExposureUS)

Additional UK labour market context source used during project design:

- NFER Skills Imperative 2035: [https://www.nfer.ac.uk/publications/skills-imperative-2035-final-report/](https://www.nfer.ac.uk/publications/skills-imperative-2035-final-report/)
