{# run as last post-hook in model #}
{%- macro insert_unknown_member() %}
{%- set columns = get_columns_in_relation(this) %}
{% if is_incremental() %} 
{%- set delete_sql %}
DELETE FROM {{ this }} WHERE $1 = '-1'
{% endset -%}
{% do run_query(delete_sql) %}
{% endif %}
INSERT INTO {{ this }}
SELECT
    {%- for col in columns %}
        {%- if '_SID' in col.name.upper() %}
    '-1'  AS {{ col.name.lower() }}
        {%- elif col.name.upper() in ['DW_INSERT_DATE', 'DW_MODIFIED_DATE'] %}
    CURRENT_TIMESTAMP() AS {{ col.name.lower() }}
        {%- elif 'CHARACTER' in col.data_type.upper() and col.string_size() >= 7  %}
    'UNKNOWN' AS {{ col.name.lower() }}
        {%- elif 'CHARACTER' in col.data_type.upper() and col.string_size() >= 3  %}
    'UNK' AS {{ col.name.lower() }}
        {%- elif 'NUMBER' in  col.data_type %}
    NULL AS {{ col.name.lower() }}
        {%- else %}
    NULL AS {{ col.name.lower() }}
        {%- endif %}
        {%- if not loop.last %},{%- endif %} 
    {%- endfor %}
{%- endmacro %}