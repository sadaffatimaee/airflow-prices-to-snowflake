# HW4: Airflow – Alpha Vantage → Snowflake (Full Refresh)

## (+2) Tasks with `@task` Decorator
I created six tasks with the `@task` decorator:
- `get_api_key`
- `extract_90`
- `ensure_table`
- `load_transactional`
- `verify_counts`
- `log_summary`

They are chained in the following order:  
`ensure_table >> extract_90 >> load_transactional >> verify_counts >> log_summary`

---

## (+1) Variables
I set up the following Airflow Variables:
- `ALPHAVANTAGE_API_KEY` → stores the API key
- `HW4_SYMBOL` → default symbol (AAPL)

Code usage:
```python
Variable.get("ALPHAVANTAGE_API_KEY")
Variable.get("HW4_SYMBOL", default_var="AAPL")
