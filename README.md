# Airflow-DBT Generic Project
This guide will walk you through setting up the required dependencies for having an Airflow-DBT project, and demo some useful features in this repository. 

## Contents
1. Setup
2. Airflow dag examples (and subdags)
3. Airflow DBT operator examples
4. DBT pre-run hooks and macros
5. DBT `fact_foreign_key_combinations` (for Tableau visualizations)
6. Git pre-commit hooks
7. Astronomer guides (continuous deployment)
8. Integration with Airbyte
9. Slack alerts on task failure/success

## Setup
1. Download latest version of [Astronomer CLI](https://github.com/astronomer/astro-cli/releases/#assets) (select `windows_386`)
2. Add `astro.exe` to Path
3. Run `python -m venv dbt-env`
4. Run `pip install -r requirements.txt`
5. Activate the `dbt-env` and inside the `/dags` directory, run `dbt init`
6. Check that airflow runs smoothly `astro dev start`

## Airflow Dag Examples




