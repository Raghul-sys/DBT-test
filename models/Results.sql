select
  *
from
  (
    select
      TO_CHAR(MonthDate, 'YYYY-MM') as MonthDate
      , Last_month_status
      , this_month_status
      , transitions
    from
      select * from {{ ref('Standard_cohorts') }}
    union all
    select
      TO_CHAR(MonthDate, 'YYYY-MM') as MonthDate
      , Last_month_status
      , this_month_status
      , transitions
    from
      select * from {{ ref('reacquired') }}
    union all
    select
      TO_CHAR(Monthdate_1, 'YYYY-MM') as MonthDate
      , Last_month_status
      , this_month_status
      , transitions
    from
      select * from {{ ref('Zombie') }}
  )
  as temp
order by
  MonthDate desc
  , this_month_status asc
  , Last_month_status asc