-- Intermediate — one row per team per match (the home/away expansion).
--
-- Each played match becomes two rows: the match from the home team's point of
-- view and from the away team's. This is the reusable team-match grain the
-- standings mart aggregates over; future marts (form guide, home/away splits,
-- aproveitamento por mando) can build on the same base instead of re-deriving
-- the union.
--
-- `mando` ('mandante'/'visitante') is kept so downstream models can split by
-- home/away without re-reading the source.
{{ config(tags=['brasileirao']) }}

with base as (
    select * from {{ ref('stg_silver_brasileirao') }}
),

mandante as (
    select
        season,
        home_team as time_,
        cast('mandante' as varchar) as mando,
        score_home as gols_pro,
        score_away as gols_contra,
        case when score_home > score_away then 1 else 0 end as vitoria,
        case when score_home = score_away then 1 else 0 end as empate,
        case when score_home < score_away then 1 else 0 end as derrota,
        case
            when score_home > score_away then 3
            when score_home = score_away then 1
            else 0
        end as pontos
    from base
    where score_home is not null
),

visitante as (
    select
        season,
        away_team as time_,
        cast('visitante' as varchar) as mando,
        score_away as gols_pro,
        score_home as gols_contra,
        case when score_away > score_home then 1 else 0 end as vitoria,
        case when score_away = score_home then 1 else 0 end as empate,
        case when score_away < score_home then 1 else 0 end as derrota,
        case
            when score_away > score_home then 3
            when score_away = score_home then 1
            else 0
        end as pontos
    from base
    where score_away is not null
)

select * from mandante
union all
select * from visitante
