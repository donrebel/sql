with 
-- initial dataset
init as (
select
  *
from unnest([
  struct(1 as case_id, 'team A' AS team, 'ta_a' as owner),
  struct(2 as case_id, 'team A' AS team, 'ta_b' as owner),
  struct(3 as case_id, 'team A' AS team, 'ta_c' as owner),
  struct(4 as case_id, 'team A' AS team, 'ta_d' as owner),
  struct(5 as case_id, 'team A' AS team, 'ta_a' as owner),

  struct(6 as case_id, 'team B' AS team, 'tb_a' as owner),
  struct(7 as case_id, 'team B' AS team, 'tb_b' as owner),
  struct(8 as case_id, 'team B' AS team, 'tb_c' as owner),
  struct(9 as case_id, 'team B' AS team, 'tb_d' as owner),
  struct(10 as case_id, 'team B' AS team, 'tb_a' as owner),
  struct(11 as case_id, 'team B' AS team, 'tb_a' as owner),

  struct(12 as case_id, 'team C' AS team, 'tc_a' as owner),
  struct(13 as case_id, 'team C' AS team, 'tc_b' as owner),
  struct(14 as case_id, 'team C' AS team, 'tc_c' as owner),
  
  struct(15 as case_id, 'team D' AS team, 'td_d' as owner),
  struct(16 as case_id, 'team D' AS team, 'td_a' as owner)])
),
-- calculating weights of team buckets
team_buckets as (
  select
    team,
    count(1) as case_count
  from 
    init
  group by 
    1    
),
-- flagging the first 50% of cases for each team as assigned
first_stage_assignment as (
  select 
    rand() as rnd,
    if(row_number() over(partition by a.team) <= b.case_count / 2, True, False) as assigned,
    a.case_id,
    a.team, 
    a.owner
  from 
    init as a
  left join
    team_buckets as b
  on a.team = b.team
),
-- calculating assignments for the remaining cases based on the random tocken and the weight of the reviewer team bucket, excluding the case owner team from the calculation
second_stage_assignment AS (
  select
    rnd,
    case_id,
    owner_team,
    reviewer_team,
    ifnull(lag(cumulative_percentage) over(partition by case_id order by cumulative_percentage), 0) as p_low,
    if(cumulative_percentage = 1, 1.1, cumulative_percentage) as p_high
  from (
    select
      rnd,
      case_id,
      c.team as owner_team,
      r.team as reviewer_team,
      r.case_count,
      r.case_count / sum(r.case_count) over (partition by case_id),
      sum(r.case_count) over (partition by case_id order by r.team) / sum(r.case_count) over (partition by case_id) as cumulative_percentage
     from (
      select
        *
      from 
        first_stage_assignment as c
      where 
        assigned = False
    ) as c
    cross join
      team_buckets as r
    where
      c.team != r.team
  )
)
-- the final unionned dataset
select
 *
from (
select
  case_id,
  owner_team,
  reviewer_team
from 
  second_stage_assignment
where
  p_low <= rnd and rnd < p_high

union all

select
  case_id,
  team as owner_team,
  team as reviewer_team
from 
  first_stage_assignment
where
  assigned
) 
order by case_id
