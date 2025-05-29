{{ config(materialized='table') }}

with players as (
    select distinct player_of_match as player_name from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_1 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_2 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_1 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_2 from {{ ref('stg_ipl_matches') }}   
    union
    select distinct team_1_player_3 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_4 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_3 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_4 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_5 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_6 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_5 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_6 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_7 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_7 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_8 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_8 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_9 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_10 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_2_player_11 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_9 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_10 from {{ ref('stg_ipl_matches') }}
    union
    select distinct team_1_player_11 from {{ ref('stg_ipl_matches') }}
    
)

select
    {{ dbt_utils.generate_surrogate_key(['player_name']) }} as player_id,
    player_name
from players
where player_name is not null
