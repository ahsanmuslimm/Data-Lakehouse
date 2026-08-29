{% macro generate_surrogate_key(column_list) %}
  {% set concatenated %}
    {% for col in column_list %}
      COALESCE(CAST({{ col }} AS TEXT), 'Unknown')
      {% if not loop.last %} || '-' || {% endif %}
    {% endfor %}
  {% endset %}

  CASE 
    WHEN {{ concatenated }} = 'Unknown' THEN '-1'
    ELSE md5({{ concatenated }})
  END
{% endmacro %}
