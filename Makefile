.PHONY: dbt-run
dbt-run:
	dbt run --profiles-dir test_dbt_duckdb/profiles/ --project-dir test_dbt_duckdb