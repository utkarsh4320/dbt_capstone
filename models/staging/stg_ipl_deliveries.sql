{{ config(
    materialized='view' 
) }}

with raw as (
    select * from {{ source('raw', 'IPL_MATCH_DATA') }}
),

unnested as (

    select
        match_id,
        innings as innings_id,
        DELIVERY_NUMBER as ball_number,
        OVER_NUMBER,
        batter,
        bowler,
        team,
        team_1,
        team_2,
        non_striker,
        runs_batter,
        runs_extras,
        runs_total,
        extras_legbyes,
        extras_noballs,
        extras_byes,
        extras_wides,
        extras_penalty,
        wicket_kind,
        wicket_player_out,
        WICKET_FIELDER_1 as fielder_involved_1,
        WICKET_FIELDER_2 as fielder_involved_2,
        review_type,
        review_decision,
        review_by,
        REPLACEMENT_IN,
        REPLACEMENT_OUT
        
    from raw
    -- if flattening json, use:
    -- , lateral flatten(input => raw.deliveries) f
)

select
    match_id,
    innings_id,
    OVER_NUMBER,
    ball_number,
    batter,
    bowler,
    non_striker,
    team as batting_team,
    case when team = team_1 then team_2 else team_1 end as bowling_team,
    cast(runs_batter as integer) as runs_batter,
    cast(runs_extras as integer) as runs_extras,
    cast(runs_total as integer) as runs_total,

    cast(extras_legbyes as integer) as extras_legbyes,
    cast(extras_noballs as integer) as extras_noballs,
    cast(extras_byes as integer) as extras_byes,
    cast(extras_wides as integer) as extras_wides,
    cast(extras_penalty as integer) as extras_penalty,
    wicket_kind,
    wicket_player_out,
    fielder_involved_1,
    fielder_involved_2,
    review_type,
    review_decision,
    review_by,
    REPLACEMENT_IN,
    REPLACEMENT_OUT

from unnested
