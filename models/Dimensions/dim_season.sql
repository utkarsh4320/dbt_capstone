{{ config(materialized='table') }}

select
    season,
    min(match_date) as season_start_date,
    max(match_date) as season_end_date,
    count(distinct match_id) as total_matches
from {{ ref('stg_ipl_matches') }}
where season is not null
group by season
