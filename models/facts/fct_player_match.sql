{{ config(materialized='table') }}

select
    match_id,
    batter_id as player_id,
    sum(runs_batter) as runs_scored,
    count(*) as balls_faced,
    sum(case when runs_batter = 4 then 1 else 0 end) as fours,
    sum(case when runs_batter = 6 then 1 else 0 end) as sixes,
    sum(case when player_out_id = batter_id then 1 else 0 end) as is_out

from {{ ref('fct_deliveries') }}
group by match_id, batter_id
