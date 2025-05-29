{{ config(materialized='table') }}
with deliveries as (
    select
        d.match_id,
        m.season,
        d.over_number,
        d.batter,
        {{ dbt_utils.generate_surrogate_key(['batter']) }} as batter_id,
        d.bowler,
        {{ dbt_utils.generate_surrogate_key(['bowler']) }} as bowler_id,
        d.runs_batter,
        d.runs_total,
        d.wicket_kind,
        extras_legbyes,
        extras_noballs,
         extras_byes,
         extras_wides,
         extras_penalty,
        case when d.runs_batter = 4 then 1 else 0 end as is_four,
        case when d.runs_batter = 6 then 1 else 0 end as is_six,
        case 
            when coalesce(d.extras_wides, 0) = 0 
             and coalesce(d.extras_noballs, 0) = 0 
            then 1 
            else 0 
        end as is_valid_delivery,
        case
            when d.over_number between 1 and 6 then 'Powerplay'
            when d.over_number between 16 and 20 then 'Death'
            else 'Middle'
        end as phase
    from {{ ref('stg_ipl_deliveries') }} d
    join {{ ref('dim_match') }} m on d.match_id = m.match_id
),

batting_phase as (
    select
        batter_id,
        season,
        phase,
        sum(runs_batter) as phase_runs,
        round(sum(runs_batter) * 1.0 / nullif(sum(is_valid_delivery), 0) * 100, 2) as phase_strike_rate
    from deliveries
    group by batter_id, season, phase
),

batting as (
    select
        batter_id,
        season,
        count(distinct match_id) as matches_played,
        count(*) as innings_played,
        sum(runs_batter) as total_runs,
        sum(case when wicket_kind is not null then 1 else 0 end) as dismissals,
        round(sum(runs_batter) * 1.0 / nullif(sum(case when wicket_kind is not null then 1 else 0 end), 0), 2) as batting_avg,
        round(sum(runs_batter) * 1.0 / nullif(sum(is_valid_delivery), 0) * 100, 2) as strike_rate,
        sum(case when runs_batter >= 100 then 1 else 0 end) as centuries,
        sum(case when runs_batter >= 50 and runs_batter < 100 then 1 else 0 end) as fifties,
        sum(is_four) as fours,
        sum(is_six) as sixes
    from deliveries
    group by batter_id, season
),

bowling as (
    select
        bowler_id,
        season,
        count(*) as balls_bowled,
        sum(case when wicket_kind is not null then 1 else 0 end) as wickets,
        sum(runs_total) as runs_conceded,
        round(sum(runs_total) * 6.0 / nullif(count(*), 0), 2) as economy_rate
    from deliveries
    where is_valid_delivery = 1
    group by bowler_id, season
),

player_summary as (
    select
        coalesce(b.batter_id, bo.bowler_id) as player_id,
        p.player_name,
        coalesce(b.season, bo.season) as season,

        -- Batting stats
        b.matches_played,
        b.innings_played,
        b.total_runs,
        b.dismissals,
        b.batting_avg,
        b.strike_rate,
        b.centuries,
        b.fifties,
        b.fours,
        b.sixes,

        -- Bowling stats
        bo.balls_bowled,
        bo.wickets,
        bo.runs_conceded,
        bo.economy_rate
    from batting b
    full outer join bowling bo on b.batter_id = bo.bowler_id and b.season = bo.season
    left join {{ ref('dim_player') }} p on coalesce(b.batter_id, bo.bowler_id) = p.player_id
),

career_totals as (
    select
        player_id,
        player_name,
        'Career' as season,
        sum(matches_played) as matches_played,
        sum(innings_played) as innings_played,
        sum(total_runs) as total_runs,
        sum(dismissals) as dismissals,
        round(sum(total_runs) * 1.0 / nullif(sum(dismissals), 0), 2) as batting_avg,
        round(sum(total_runs) * 1.0 / nullif(nullif(sum(balls_bowled), 0), 0) * 100, 2) as strike_rate,
        sum(centuries) as centuries,
        sum(fifties) as fifties,
        sum(fours) as fours,
        sum(sixes) as sixes,
        sum(balls_bowled) as balls_bowled,
        sum(wickets) as wickets,
        sum(runs_conceded) as runs_conceded,
        round(sum(runs_conceded) * 6.0 / nullif(sum(balls_bowled), 0), 2) as economy_rate
    from player_summary
    group by player_id, player_name
),

final_union as (
    select * from player_summary
    union all
    select * from career_totals
)

select * from final_union
