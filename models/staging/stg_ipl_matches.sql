{{ config(
    materialized='view'  
) }}

with raw as (
    select * from {{ source('raw', 'IPL_MATCH_DATA') }}
),

renamed as (
    select
        match_id,
        cast(balls_per_over as integer) as balls_per_over,
        city,
        coalesce(dates_1, dates_2, dates_3, dates_4, dates_5, dates_6) as match_date,
        event_name,
        match_number,
        event_stage,
        gender,
        match_type,
        match_type_number,

        -- Match outcome
        match_result,
        method,
        winner,
        by_runs,
        by_wickets,
        by_innings,

        -- Toss info
        toss_winner,
        toss_decision,

        -- Venue info
        venue,

        -- Season & teams
        case when season = '2007/08' then '2008' 
        when season ='2009/10' then '2010' 
        when season ='2020/21' then '2021' else season end as season,
        team_type,
        team_1,
        team_2,

        -- Player of match
        player_of_match,
        player_of_match_id,

        -- Umpires and referees
        umpire_1,
        umpire_2,
        tv_umpires,
        reserve_umpires,
        match_referees,
        umpire_1_id,
        umpire_2_id,
        tv_umpires_id,
        reserve_umpires_id,
        match_referees_id,

        -- Lineups (limited to first 4 here, you can expand to all 11 later)
        team_1_player_1, team_1_player_1_id,
        team_1_player_2, team_1_player_2_id,
        team_1_player_3, team_1_player_3_id,
        team_1_player_4, team_1_player_4_id,
        team_1_player_5, team_1_player_5_id,
        team_1_player_6, team_1_player_6_id,
        team_1_player_7, team_1_player_7_id,
        team_1_player_8, team_1_player_8_id,
        team_1_player_9, team_1_player_9_id,
        team_1_player_10, team_1_player_10_id,
        team_1_player_11, team_1_player_11_id,
        team_1_player_12, team_1_player_12_id,

        team_2_player_1, team_2_player_1_id,
        team_2_player_2, team_2_player_2_id,
        team_2_player_3, team_2_player_3_id,
        team_2_player_4, team_2_player_4_id,
        team_2_player_5, team_2_player_5_id,
        team_2_player_6, team_2_player_6_id,
        team_2_player_7, team_2_player_7_id,
        team_2_player_8, team_2_player_8_id,
        team_2_player_9, team_2_player_9_id,
        team_2_player_10, team_2_player_10_id,
        team_2_player_11, team_2_player_11_id,
        team_2_player_12, team_2_player_12_id,

        -- Row number to deduplicate
        row_number() over (partition by match_id order by coalesce(dates_1, dates_2, dates_3)) as row_num
    from {{ source('raw', 'IPL_MATCH_DATA') }}
)

select * from renamed
where row_num = 1
