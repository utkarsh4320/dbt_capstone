{{ config(materialized='table') }}

with teams as (
    select distinct
        team_1 as team_name
    from {{ ref('stg_ipl_matches') }}
    union
    select distinct
        team_2 as team_name
    from {{ ref('stg_ipl_matches') }}
    union
    select distinct
        toss_winner as team_name
    from {{ ref('stg_ipl_matches') }}
    union
    select distinct
        winner as team_name
    from {{ ref('stg_ipl_matches') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_id,
    team_name
from teams
where team_name is not null
