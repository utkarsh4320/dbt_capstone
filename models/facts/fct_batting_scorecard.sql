with deliveries as (
    select
        d.match_id,
        m.season,
        d.innings_id as innings,
        d.batter,
        d.bowler,
        d.runs_batter,
        d.runs_total,
        d.wicket_kind as dismissal_kind,
        d.over_number,
        d.ball_number,
        case when d.runs_batter = 4 then 1 else 0 end as is_four,
        case when d.runs_batter = 6 then 1 else 0 end as is_six,
        case 
            when coalesce(d.extras_wides, 0) = 0 
             and coalesce(d.extras_noballs, 0) = 0 
            then 1 
            else 0 
        end as is_valid_delivery
    from {{ ref('stg_ipl_deliveries') }} d
    join {{ ref('dim_match') }} m on d.match_id = m.match_id
)

select
    match_id,
    season,
    innings,
    {{ dbt_utils.generate_surrogate_key(['batter']) }} as batter_id,
    count(*) as balls_faced,
    sum(is_valid_delivery) as legal_deliveries_faced,
    sum(runs_batter) as total_runs,
    sum(is_four) as fours,
    sum(is_six) as sixes,
    sum(case when dismissal_kind is not null then 1 else 0 end) as dismissals,
    round(sum(runs_batter) * 100.0 / nullif(sum(is_valid_delivery), 0), 2) as strike_rate
from deliveries
group by match_id, season, innings, batter_id
