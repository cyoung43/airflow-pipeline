FROM quay.io/astronomer/ap-airflow:2.3.0
ENV AIRFLOW_VAR_SNOWFLAKE_ACCOUNT=PN17132
ENV AIRFLOW_VAR_SNOWFLAKE_USERNAME=airflow
ENV AIRFLOW_VAR_SNOWFLAKE_PASSWORD=Airflow12
ENV AIRFLOW_VAR_SNOWFLAKE_TRANSFORM_WAREHOUSE_NAME=COMPUTE_WH
# Add additional variables as needed
RUN pip install snowflake-connector-python==2.7.2
RUN pip install dbt-snowflake==1.0.0
RUN pip install airflow-dbt==0.4.0
# pyyaml==6.0
RUN pip install apache-airflow-providers-http
RUN pip install apache-airflow-providers-airbyte
RUN pip install apache-airflow-providers-slack
RUN pip install apache-airflow-providers-slack[http]