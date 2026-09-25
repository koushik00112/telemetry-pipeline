{% test unique_combination(model, columns) %}
-- Fails with one row per duplicated key combination.
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
