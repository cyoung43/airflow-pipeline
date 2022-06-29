# Airflow-DBT Generic Project
This guide will walk you through setting up the required dependencies for having an Airflow-DBT project, and demo some useful features in this repository.

## TO DO
1. Change DBT package to `airflow-dbt-python`
2. Finish readme

## Contents
1. [Setup](#setup)
2. [Airflow dag examples](#airflow-dag-examples)
3. [Airflow DBT operator examples](#airflow-dbt-operators)
4. [DBT pre-run hooks](#dbt-pre-run-hooks)
5. [DBT macros](#dbt-macros)
6. [Git pre-commit hooks](#git-pre-commit-hook)
7. [Astronomer guides](#astronomer-cd) (continuous deployment)
9. [Slack alerts](#slack-alerts) on task failure/success

## Setup
1. Download latest version of [Astronomer CLI](https://github.com/astronomer/astro-cli/releases/#assets) (select `windows_386`)
    * Astrocloud is now the recommended cli tool for astronomer local deployments.
2. Add `astro.exe` to Path
3. Run `python -m venv dbt-env`
4. Run `pip install -r requirements.txt`
5. Activate the `dbt-env` and inside the `/dags` directory, run `dbt init`
6. Check that airflow runs smoothly `astro dev start`

## Airflow Dag Examples

## Airflow DBT operators

## DBT pre-run hooks

## Git pre-commit hook

## DBT macros

## DBT Tests

## Astronomer CD

## Slack alerts
