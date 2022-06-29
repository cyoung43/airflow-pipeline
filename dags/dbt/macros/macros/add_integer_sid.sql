{%- macro add_integer_sid(primary_relation, foreign_relations, input_sid, output_sid, run_on_dev=False) -%}
{# - primary_relation: name of the model that you are taking the primary key/SID value from
- foreign_relations: a list of any models that relate to that primary key
- input_sid: the name of the primary key/SID value (this needs to be the same across all models you are referencing)
- output_sid: the name of the new numeric column
 #}
{{ log("Running sid to integer conversion for: " ~ primary_relation, True) }} {# 2nd parameter true makes this write to output #}

{%- if target.name != 'dev' or run_on_dev == True -%}

{# Create sequence #}
{%- set seq_name -%}
{{ target.database }}.{{ target.schema}}.{{ primary_relation}}_SEQ  
{% endset %}

{%- set seq_sql %}
CREATE OR REPLACE SEQUENCE {{ seq_name }}  
{% endset -%}

{%- do run_query(seq_sql) -%}

{# Add column to primary relation #}
{% set select_cols = dbt_utils.star(from=ref(primary_relation), except=[output_sid]) %}

{% set primary_sql -%}
CREATE OR REPLACE TABLE {{ target.database }}.{{ target.schema}}.{{ primary_relation}} AS
SELECT IFF({{ input_sid }} = '-1', -1, {{ seq_name }}.nextval) AS {{ output_sid }},
{{ select_cols }}
FROM {{ target.database }}.{{ target.schema}}.{{ primary_relation}}  
{%- endset %}

{# {{ log(primary_sql, True) }} #}
{%- do run_query(primary_sql) -%}

{# Add column to foreign relations #}
{%- for rel in foreign_relations %}

    {# {{ log("table_name: " ~ rel, True) }} #}

    {%- set select_cols = dbt_utils.star(from=ref(rel), relation_alias = 'f', except=[output_sid]) -%}

    {%- set foreign_sql %}
    CREATE OR REPLACE TABLE {{ target.database }}.{{ target.schema}}.{{ rel }} AS
    SELECT COALESCE(p.{{ output_sid }}, -1) AS {{ output_sid }},
    {{ select_cols }}
    FROM {{ target.database }}.{{ target.schema}}.{{ rel }} f 
    LEFT JOIN {{ target.database }}.{{ target.schema}}.{{ primary_relation}} p
    ON f.{{ input_sid }} = p.{{ input_sid }}
    {% endset -%}

    {# {{ log(foreign_sql, True) }} #}
    {%- do run_query(foreign_sql) -%}
    {# {{ log('Complete', True) }} #}

{% endfor -%}

{%- endif -%}
{%- endmacro -%}
