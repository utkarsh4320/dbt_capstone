{{ config(materialized='table') }}

select
    match_id,
    innings_id,
    {{ dbt_utils.generate_surrogate_key(['batting_team']) }} as batting_team_id,
    {{ dbt_utils.generate_surrogate_key(['bowling_team']) }} as bowling_team_id,
    count(*) as balls_faced,
    sum(runs_batter) as total_runs_batter,
    sum(runs_extras) as total_extras,
    sum(runs_total) as total_score,
    sum(case when wicket_kind is not null then 1 else 0 end) as total_wickets,

    min(OVER_NUMBER) as first_over,
    max(OVER_NUMBER) as last_over

from {{ ref('fct_deliveries') }}
left join {{ ref('stg_ipl_matches') }} using (match_id)
group by match_id, innings_id, batting_team_id, bowling_team_id
