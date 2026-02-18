{% macro bronze_load(periods) %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS bronze") %}
  {% for p in periods %}
    {% set table_name = p.type ~ '_' ~ p.period %}
    {% set parquet_path = '/opt/sppd/data/parquet/' ~ p.type ~ '/' ~ p.period ~ '/*.parquet' %}
    {% set sql %}
      CREATE OR REPLACE TABLE bronze.{{ table_name }} AS
      SELECT * FROM read_parquet('{{ parquet_path }}')
    {% endset %}
    {% do run_query(sql) %}
  {% endfor %}
{% endmacro %}
