-- docs: https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18

{% macro create_item_parse() %}

create or replace function item_parse(ITEM variant, CONFIGURABLE_AS string)
returns text
language javascript
as
$$
    if (ITEM) {
        if (CONFIGURABLE_AS === 'single_item') {
            return Object.keys(ITEM)[0]
        }
        
        const count = Object.values(ITEM)[0]
        
        if (count) {
            return count > 1 ? `${count} items` : `${count} item`
        }
    }
    return null
$$

{% endmacro %}