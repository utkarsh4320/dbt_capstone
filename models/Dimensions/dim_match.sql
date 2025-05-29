{{ config(materialized='table') }}

select
    match_id,
    match_number,
    match_type,
    match_type_number,
    match_date,
    season,
    team_type,
    event_name,
    event_stage,
    gender
from {{ ref('stg_ipl_matches') }}
where match_id is not null
