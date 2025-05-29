{{ config(materialized='table') }}

with base as (
    select
        m.match_id,
        m.match_date,
        m.season,
        m.venue,

        t1.team_name as team_1,
        t2.team_name as team_2,
        tw.team_name as toss_winner,
        m.toss_decision,
        mw.team_name as match_winner,

        m.by_runs,
        m.by_wickets,
        m.method,
        m.player_of_match,

        case
            when mw.team_name is null then 'No Result'
            when m.by_runs > 0 then concat(m.by_runs, ' runs')
            when m.by_wickets > 0 then concat(m.by_wickets, ' wickets')
            else 'Tied'
        end as result_margin,

        case
            when mw.team_name is null then 'No Result'
            when m.method is not null then 'DLS'
            else 'Normal'
        end as result_type,

        -- innings 1 performance
        i1.batting_team_id as team_1_batting_id,
        bt1.team_name as team_1_batting,
        i1.total_score as team_1_score,
        i1.total_wickets as team_1_wickets,

        -- innings 2 performance
        i2.batting_team_id as team_2_batting_id,
        bt2.team_name as team_2_batting,
        i2.total_score as team_2_score,
        i2.total_wickets as team_2_wickets

    from {{ ref('fct_match_results') }} m
    left join {{ ref('dim_team') }} t1 on m.team_1_id = t1.team_id
    left join {{ ref('dim_team') }} t2 on m.team_2_id = t2.team_id
    left join {{ ref('dim_team') }} tw on m.toss_winner_team_id = tw.team_id
    left join {{ ref('dim_team') }} mw on m.winner_team_id = mw.team_id

    left join {{ ref('fct_innings') }} i1 on m.match_id = i1.match_id and i1.innings_id = 1
    left join {{ ref('fct_innings') }} i2 on m.match_id = i2.match_id and i2.innings_id = 2

    left join {{ ref('dim_team') }} bt1 on i1.batting_team_id = bt1.team_id
    left join {{ ref('dim_team') }} bt2 on i2.batting_team_id = bt2.team_id
)

select * from base
