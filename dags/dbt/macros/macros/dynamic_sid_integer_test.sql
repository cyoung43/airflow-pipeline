{%- macro dynamic_sid_integer_test(dim, fact, sid, sid_int) -%}

{% set select_cols = dbt_utils.star(from=ref(dim),relation_alias='d') %}

Select CONCAT_WS(', ', {{select_cols}})
FROM {{ ref(dim) }} d
LEFT JOIN {{ ref(fact) }} f
    on d.{{ sid }} = f.{{ sid }}
where d.{{ sid_int }} != f.{{ sid_int }}

{%- endmacro -%}