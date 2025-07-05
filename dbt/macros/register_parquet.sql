{% macro register_parquet(path, alias) -%}
create or replace view {{ alias }} as
select * from read_parquet('{{ path }}');
{%- endmacro %}
