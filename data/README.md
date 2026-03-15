# Optional Enrichment Inputs

These files are optional but strongly recommended for higher-quality UK outputs.

- `soc_crosswalk.csv`
  - columns: `slug,soc_code`
- `ashe.csv`
  - columns: `soc_code,median_pay_annual,median_pay_hourly`
- `labour_demand.csv`
  - columns: `soc_code,year,value`

If these are missing, `make_csv_uk.py` will still run using profile-text fallbacks, but coverage for SOC/pay/jobs/outlook will be lower.
