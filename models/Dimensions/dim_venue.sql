{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['venue']) }} as venue_id,
    venue as venue_name,
    city
from {{ ref('stg_ipl_matches') }}
where venue is not null
group by venue, city
