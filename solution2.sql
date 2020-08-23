with init as (
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
team_buckets1 as (
  select
    team,
    case_count,
    bucket_size_percentage,
    sum(bucket_size_percentage) over (order by team) as cummulative_bucket_size_percentage
  from (
    select
      team,
      case_count,
      case_count / sum(case_count) over() as bucket_size_percentage
    from (
      select
        team,
        count(1) as case_count
      from 
        init
      group by 
        1    
    )
  )
),
team_buckets2 as (
  select
    team,
    case_count,
    bucket_size_percentage,
    ifnull(lag(cummulative_bucket_size_percentage) over (order by team), 0) as b_low,
    IF(cummulative_bucket_size_percentage = 1, 1.1, cummulative_bucket_size_percentage) as b_high
  from 
    team_buckets1  
),
first_stage_assignment as (
  select 
    if(row_number() over(partition by a.team) <= b.case_count / 2, True, False) as assigned,
    a.case_id,
    a.team, 
    a.owner
  from 
    init as a
  left join
    team_buckets2 as b
  on a.team = b.team
),
second_stage_assignment AS (
  select
    case_id,
    c.team as owner_team,
    t.team as reviewer_team,
    c.rnk,
    t.b_low,
    t.b_high
  from (
    select
      percent_rank() over (order by rand()) as rnk,
      c.*,
    from 
      first_stage_assignment as c
    where 
      assigned = False
  ) as c
  cross join
    team_buckets2 as t
  where
    t.b_low <= c.rnk and c.rnk < t.b_high
)

select
 *
from (
select
  case_id,
  owner_team,
  reviewer_team
from 
  second_stage_assignment

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

