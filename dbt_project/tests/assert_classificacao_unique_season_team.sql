-- Grain guard for the standings mart: exactly one row per (season, team).
-- `time_` is not unique on its own (the same club appears in every season),
-- so the built-in `unique` test can't express this — a singular test does.
-- Passes when it returns zero rows.
{{ config(tags=['brasileirao']) }}

select
    season,
    time_,
    count(*) as n_rows
from {{ ref('mart_classificacao_brasileirao') }}
group by season, time_
having count(*) > 1
