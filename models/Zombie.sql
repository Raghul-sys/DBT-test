-- filter the records from main_cleaned, based on user who do not have events for more then 1 month, 
-- and new user who do not have events in next month(Max/final month is excluded, incase if new user appears on last month)

with Zombie as (
  select
    MonthDate
    , USER_ID
    , previous_date
    , Next_date
    , diff_days
    , Last_month_status
    , this_month_status
  from
    ( select * from {{ ref('aggregates') }} ) as t
  where
    diff_days > 35
    or (
      previous_date = Next_date
      and MonthDate < (
        select
          max(MonthDate)
        from
          main_cleaned
      )
    )
)

-- For the records with days diffrence than 30 days, add a month to previous_date and count them as zombie transition and use Last_month_status as Last_month_status.
-- If user is new in curent month and next month is not there then add a month to previous_date(same as current date) and count them as zombie, and use this_month_status as Last_month_status.

, Zombie_Cleaned as (
  select
    Monthdate_1
    , Last_month_status
    , this_month_status
    , count(user_id) as transitions
  from
    (
      select
        MonthDate
        , previous_date
        , dateadd(month, 1, previous_date) as Monthdate_1
        , case
          when MonthDate = Next_date
            then this_month_status
          else Last_month_status
        end as Last_month_status
        , 'zombie' as this_month_status
        , user_id
      from
        zombie
    )
    a
  group by
    1
    , 2
    , 3
)

select * from Zombie_Cleaned

