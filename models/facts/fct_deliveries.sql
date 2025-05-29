{{ config(materialized='incremental', unique_key='match_id || innings_id || over || ball_number') }}

select
    match_id,
    innings_id,
    OVER_NUMBER,
    ball_number,
    batting_team,
    bowling_team,
    {{ dbt_utils.generate_surrogate_key(['batter']) }} as batter_id,
    {{ dbt_utils.generate_surrogate_key(['bowler']) }} as bowler_id,
    {{ dbt_utils.generate_surrogate_key(['non_striker']) }} as non_striker_id,

    runs_batter,
    runs_extras,
    runs_total,

    extras_legbyes,
    extras_noballs,
    extras_byes,
    extras_wides,
    extras_penalty,

    case when wicket_kind is not null then 1 else 0 end as is_wicket,
    wicket_kind,
    {{ dbt_utils.generate_surrogate_key(['wicket_player_out']) }} as player_out_id,
    fielder_involved_1,
    fielder_involved_2,

    review_type,
    review_decision,
    review_by,
    -- replaced_by,
    REPLACEMENT_IN,
    REPLACEMENT_OUT

from {{ ref('stg_ipl_deliveries') }}
