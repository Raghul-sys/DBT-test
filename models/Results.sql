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
      ( select * from {{ ref('Standard_cohorts') }} ) as t
    union all
    select
      TO_CHAR(MonthDate, 'YYYY-MM') as MonthDate
      , Last_month_status
      , this_month_status
      , transitions
    from
      ( select * from {{ ref('reacquired') }} ) as t
    union all
    select
      TO_CHAR(Monthdate_1, 'YYYY-MM') as MonthDate 
      , Last_month_status
      , this_month_status
      , transitions
    from
      ( select * from {{ ref('Zombie') }} ) as t
  )
  as temp
order by
  MonthDate desc
  , this_month_status asc
  , Last_month_status asc