-- Gold mart — Brasileirão Série A league table (classificação), per season.
--
-- Aggregates the team-match intermediate into one row per (season, team) and
-- ranks teams within each season by the official Brasileirão tie-break order:
-- points → wins → goal difference → goals for. Materialized as a full table
-- (CREATE OR REPLACE) — at ~20 rows per season the rebuild is trivial.
--
-- Extends the original standings query with empates, derrotas, gols_contra and
-- aproveitamento (points won / points possible) so it reads as a complete
-- classification table. `season` partitions the RANK so multiple editions never
-- blend. Consumers order by (season, classificacao).
{{ config(tags=['brasileirao']) }}

with partidas as (
    select * from {{ ref('int_brasileirao_partidas') }}
),

estatisticas_por_time as (
    select
        season,
        time_,
        count(*) as quantidade_de_jogos,
        sum(vitoria) as numero_de_vitorias,
        sum(empate) as empates,
        sum(derrota) as derrotas,
        sum(gols_pro) as gols_pro,
        sum(gols_contra) as gols_contra,
        (sum(gols_pro) - sum(gols_contra)) as saldo_de_gols,
        sum(pontos) as pontuacao
    from partidas
    group by season, time_
),

final as (
    select
        rank() over (
            partition by season
            order by
                pontuacao desc,
                numero_de_vitorias desc,
                saldo_de_gols desc,
                gols_pro desc
        ) as classificacao,
        season,
        time_,
        quantidade_de_jogos,
        numero_de_vitorias,
        empates,
        derrotas,
        gols_pro as numero_de_gols_pro,
        gols_contra,
        saldo_de_gols,
        pontuacao,
        -- Aproveitamento: points won as a % of points available (jogos × 3).
        round(
            (pontuacao * 100.0) / nullif(quantidade_de_jogos * 3, 0),
            2
        ) as aproveitamento
    from estatisticas_por_time
)

select * from final
