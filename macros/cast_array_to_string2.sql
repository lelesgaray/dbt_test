{% macro cast_array_to_string2(array) %}
  {{ adapter.dispatch('cast_array_to_string2') (array) }}
{% endmacro %}

{% macro bigquery__cast_array_to_string2(array) %}
    (select string_agg(cast(element as string), ',') from unnest({{ array }}) element)
{% endmacro %}