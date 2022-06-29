{# Run this with the run-operation, it is not a post hook because then it will run after the models and again after tests #}

{%- macro sids_to_integers() -%}
{{ add_integer_sid('dim_employee', ['fact_labor', 'aggregate_fact_labor'], 'EMPLOYEE_SID', 'EMPLOYEE_SID_INT', run_on_dev=True) }}
{{ add_integer_sid('dim_jobcode', ['fact_labor', 'aggregate_fact_labor'], 'JOBCODE_SID', 'JOBCODE_SID_INT', run_on_dev=True) }}
{{ add_integer_sid('dim_warehouse', ['fact_labor', 'fact_shipment', 'aggregate_fact_labor'], 'WAREHOUSE_SID', 'WAREHOUSE_SID_INT', run_on_dev=True) }}
{{ add_integer_sid('dim_shipment', ['fact_shipment'], 'SHIPMENT_SID', 'SHIPMENT_SID_INT', run_on_dev=True) }}
{{ add_integer_sid('dim_client', ['fact_shipment'], 'CLIENT_SID', 'CLIENT_SID_INT', run_on_dev=True) }}


{# on-run-end requires that the macro return a select statement #}
{{ return('SELECT 1') }}
{%- endmacro -%}