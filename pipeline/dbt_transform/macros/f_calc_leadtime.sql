{% macro leadtime_days(start_date, end_date, date_table='dim_date') %}
(
  case
    when {{ start_date }} is null or {{ end_date }} is null then null
    else
      (
          select greatest(0, count(d.date_id)-1)::int
          from {{ ref (date_table) }} d
          where d.is_working_day = TRUE
          and d.date_actual between least({{ start_date }}::date, {{ end_date }}::date)
          and greatest({{ start_date }}::date, {{ end_date }}::date)

    )
  end
)

{% endmacro %}




