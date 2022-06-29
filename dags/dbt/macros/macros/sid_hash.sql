
-- macro to create a composite hash key from given columns.
-- inputs: array of columns
-- output: string of hash key
{%- macro sid_hash(field_list) -%}
{%- set fields = [] -%}
{%- for field in field_list -%}
    {%- set _ = fields.append(
        "coalesce(" ~ field ~ "::text, '<null>')"
    ) -%}
{%- endfor -%}
sha2(concat_ws('||', {{ fields|join(', ') }} ))
{%- endmacro -%}