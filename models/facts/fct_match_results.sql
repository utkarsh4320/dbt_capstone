{{ config(materialized='table') }}

select
    match_id,
    {{ dbt_utils.generate_surrogate_key(['team_1']) }} as team_1_id,
    {{ dbt_utils.generate_surrogate_key(['team_2']) }} as team_2_id,
    {{ dbt_utils.generate_surrogate_key(['winner']) }} as winner_team_id,
    {{ dbt_utils.generate_surrogate_key(['toss_winner']) }} as toss_winner_team_id,
    toss_decision,
   case 
  when by_runs > 0 then 'won by runs'
  when by_wickets > 0 then 'won by wickets'
  else 'no result'
    end as result,
case 
  when by_runs > 0 then cast(by_runs as string) || ' runs'
  when by_wickets > 0 then cast(by_wickets as string) || ' wickets'
  else null
end as result_margin,
    method,
    by_runs,
    by_wickets,
    player_of_match,
    venue,
    match_date,
    season

from {{ ref('stg_ipl_matches') }}
