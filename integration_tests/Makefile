
test-redshift:
	dbt seed --target redshift --full-refresh
	dbt run --target redshift --full-refresh
	dbt run --target redshift
	dbt test --target redshift

test-snowflake:
	dbt seed --target snowflake --full-refresh
	dbt run --target snowflake --full-refresh
	dbt run --target snowflake
	dbt test --target snowflake

test-bigquery:
	dbt seed --target bigquery --full-refresh
	dbt run --target bigquery --full-refresh
	dbt run --target bigquery
	dbt test --target bigquery

test-all: test-redshift test-snowflake test-bigquery
	echo "Completed successfully"
