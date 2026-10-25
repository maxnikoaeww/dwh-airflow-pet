{# Generic test: the combination of columns is unique #}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as cnt
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
