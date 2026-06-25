-- Staging view over the Silver Brasileirão table (read on the Nessie `main`
-- branch via Dremio). Tagged `brasileirao` so the Gold DAG selects the
-- Brasileirão graph (`dbt build --select tag:brasileirao`).
{{ config(tags=['brasileirao']) }}

select
    ge_match_id,
    matchweek,
    match_date,
    -- Season = calendar year of the match. Brasileirão Série A runs within a
    -- single calendar year, so the played-date year unambiguously identifies
    -- the edition (2026, 2027, …). This keeps every season's table separate
    -- once more than one year is ingested — without it, a future 2027 run would
    -- blend its matches into the 2026 standings.
    extract(year from match_date) as season,
    home_team,
    away_team,
    score_home,
    score_away,
    total_goals,
    match_outcome,
    stadium_state,
    updated_at
from {{ source('nessie_silver', 'brasileirao') }} AT BRANCH main
