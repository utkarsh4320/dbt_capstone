{{ config(materialized='table') }}

with umpires as (
    select umpire_1 as umpire_name from {{ ref('stg_ipl_matches') }}
    union
    select umpire_2 from {{ ref('stg_ipl_matches') }}
    union
    select tv_umpires from {{ ref('stg_ipl_matches') }}
    union
    select reserve_umpires from {{ ref('stg_ipl_matches') }}
    union
    select match_referees from {{ ref('stg_ipl_matches') }}
)

select
   
    {{ dbt_utils.generate_surrogate_key(['umpire_name']) }} as umpire_id,
    umpire_name
from umpires
where umpire_name is not null
