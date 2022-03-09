FROM quay.io/astronomer/ap-airflow:2.2.4-onbuild
ENV AIRFLOW_VAR_SNOWFLAKE_ACCOUNT=account_name
ENV AIRFLOW_VAR_SNOWFLAKE_USERNAME=snowflake_username
ENV AIRFLOW_VAR_SNOWFLAKE_PASSWORD=snowflake_password
ENV AIRFLOW_VAR_SNOWFLAKE_TRANSFORM_WAREHOUSE_NAME=transform_warehouse_name