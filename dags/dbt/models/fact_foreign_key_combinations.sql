-- Make sure all new fact tables are added here
-- examples:
-- depends_on: {{ ref('fact_schedule_pickup') }}
-- etc...


{%- set query -%}
  with fact_tables_with_dates as (
    SELECT distinct --Get the date FKs (One row per fact table)
        lower(TABLE_NAME) as DBT_Model_Name
        ,'COALESCE(' || 
            COALESCE(Nth_Value(CASE WHEN COLUMN_NAME ilike '%date%' then COLUMN_NAME end,1) IGNORE NULLS OVER (PARTITION BY TABLE_NAME ORDER BY ORDINAL_POSITION),'NULL')
            ||',''1900-1-1'')' AS First_Date_Col
        ,'COALESCE(' || 
            COALESCE(Nth_Value(CASE WHEN COLUMN_NAME ilike '%date%' then COLUMN_NAME end,2) IGNORE NULLS OVER (PARTITION BY TABLE_NAME ORDER BY ORDINAL_POSITION),'NULL')
            ||',''1900-1-1'')' AS Second_Date_Col
        ,'COALESCE(' || 
            COALESCE(Nth_Value(CASE WHEN COLUMN_NAME ilike '%date%' then COLUMN_NAME end,3) IGNORE NULLS OVER (PARTITION BY TABLE_NAME ORDER BY ORDINAL_POSITION),'NULL')
            ||',''1900-1-1'')' AS Third_Date_Col
        ,'    ' || First_Date_Col ||'::DATE as First_Date_Col,\n'
            ||'    '||Second_Date_Col ||'::DATE as Second_Date_Col,\n'
            ||'    '||Third_Date_Col ||'::DATE as Third_Date_Col,\n'
            as Date_FK_List
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE 1=1
    and Table_Name ilike '%fact%' 
    and Table_Schema = 'DW'
    and not table_name ilike '%fact_foreign_key%' 
  )
  , full_fk_list as (
    SELECT distinct 
      COLUMN_NAME as FK_Col
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE COLUMN_NAME iLIKE '%_SID' 
    and Table_Name ilike '%fact%' 
    and Table_Schema = 'DW'
    and not table_name ilike '%fact_foreign_key%'
  ),
  actual_fact_fks as (
    SELECT 
      lower(TABLE_NAME) as Table_Name, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE COLUMN_NAME iLIKE '%_SID' 
    and Table_Name ilike '%fact%' 
    and Table_Schema = 'DW'
    and not table_name ilike '%fact_foreign_key%'
  ), table_sql as (
  Select distinct
      fact_tables_with_dates.DBT_Model_Name
      ,LISTAGG('    ' || COALESCE(actual_fact_fks.Column_Name || ' AS ','''-1'' AS ') || full_fk_list.FK_Col,',\n') 
          WITHIN GROUP (ORDER BY full_fk_list.FK_Col) 
          OVER (PARTITION BY fact_tables_with_dates.DBT_Model_Name) AS FK_List
      ,Date_FK_List || FK_List as column_sql --the break between 'from {' and '{' is to prevent the dbt compiler from reading this as a reference  
      --select count(*)
  from fact_tables_with_dates 
  full join full_fk_list
      on 1=1
  left join actual_fact_fks
      on actual_fact_fks.table_name = fact_tables_with_dates.dbt_model_name
      and actual_fact_fks.column_name = full_fk_list.fk_col
  )
  select 
    DBT_MODEL_NAME,
    COLUMN_SQL
  from table_sql
{%- endset -%}
{%- set sources = [] -%}

{%- for node in graph.nodes.values() -%}
    {%- do sources.append(node.name) -%}
{%- endfor -%}


{%- if execute -%}
  {%- set results = run_query(query) -%}
{%- else -%}
  {%- set results_list = [] -%}
{%- endif -%}
{%- set results_list = results.rows -%}

{%- set ns = namespace(first_item=True) -%}

{%- for item in results_list -%}
  {%- if (item.DBT_MODEL_NAME in sources) -%}
    {%- if not ns.first_item %}
    UNION
    {%- endif -%}
    
    {%- set ns.first_item = False %}
    select 
    {{item.COLUMN_SQL}}
    from {{ ref(item.DBT_MODEL_NAME) }}
  {% endif -%}
{%- endfor -%}