CREATE PROCEDURE SANDBOX.ANALYSIS_PRODUCT.REG_DASH_USER_FINAL_AGG_PROCEDURE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
$$


begin

DELETE FROM SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_FINAL_AGG 
 WHERE 
 (date_type = 'daily' and date >= (current_date() - 8)) 
    or
 (date_type = 'monthly' and date >= dateadd('month',-1,date_trunc('month',current_date()))) 
    or 
 (date_type = 'yearly' and date >= dateadd('year',-1,date_trunc('year',current_date())))
    or
 (date_type = 'weekly' and date >= dateadd('week',-1,date_trunc('week',current_date())))
    or
 (date_type = 'quarterly' and date >= dateadd('quarter',-1,date_trunc('quarter',current_date())))
    or
 (date_type = 'all-time' and date >= (current_date() - 8));

--------------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_FINAL_AGG 
with 
client_user_mapping as (
select
  user_key,
  min(first_time_user_client_pair_utc) as signup_date
  from
  "BI"."PLUTO_DW"."CLIENT_USER_MAPPING_VW"
  group by all
),

daily_accounts as (
select
a.date,
a.region_type,
a.region,
a.country,
a.adjusted_user_key,
max(case when date_trunc('day',a.date) = date_trunc('day',b.signup_date) then 1 else 0 end) as new_account_flag,
sum(a.tvms) as tvms,
sum(a.reg_tvms) as reg_tvms,
sum(a.loggedin_tvms) as loggedin_tvms
from
    SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK a
left join
    client_user_mapping b
on
    a.adjusted_user_key = b.user_key
where 
    a.adjusted_user_key is not null
group by all
),

daily_final as (
select
date,
'daily' as date_type,
region_type,
region,
country,
case when new_account_flag = 1 then 'new' else 'returning' end as account_tenure,
count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
from
daily_accounts
group by all
  
union
  
select
date,
'daily' as date_type,
region_type,
region,
country,
'ttl' as account_tenure,
count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
from
daily_accounts
group by all
),

monthly_accounts as (
select
date_trunc('month',a.date) as date,
a.region_type,
a.region,
a.country,
a.adjusted_user_key,
max(case when date_trunc('month',a.date) = date_trunc('month',b.signup_date) then 1 else 0 end) as new_account_flag,
sum(a.tvms) as tvms,
sum(a.reg_tvms) as reg_tvms,
sum(a.loggedin_tvms) as loggedin_tvms
from
SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK a
left join
    client_user_mapping b
on
    a.adjusted_user_key = b.user_key
where a.adjusted_user_key is not null
group by all
),

monthly_final as (
select
date,
  'monthly' as date_type,
  region_type,
  region,
  country,
  case when new_account_flag = 1 then 'new' else 'returning' end as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from monthly_accounts
  group by all
  
  union
  
select
date,
  'monthly' as date_type,
  region_type,
  region,
  country,
  'ttl' as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from monthly_accounts
  group by all
),

yearly_accounts as (
select
date_trunc('year',a.date) as date,
a.region_type,
a.region,
a.country,
a.adjusted_user_key,
max(case when date_trunc('year',a.date) = date_trunc('year',b.signup_date) then 1 else 0 end) as new_account_flag,
sum(a.tvms) as tvms,
sum(a.reg_tvms) as reg_tvms,
sum(a.loggedin_tvms) as loggedin_tvms
from
SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK a
left join
    client_user_mapping b
on
    a.adjusted_user_key = b.user_key
where a.adjusted_user_key is not null
group by all
),

yearly_final as (
select
date,
  'yearly' as date_type,
  region_type,
  region,
  country,
  case when new_account_flag = 1 then 'new' else 'returning' end as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from yearly_accounts
  group by all
  
union
  
select
date,
  'yearly' as date_type,
  region_type,
  region,
  country,
  'ttl' as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from yearly_accounts
  group by all
),

quarterly_accounts as (
select
date_trunc('quarter',a.date) as date,
a.region_type,
a.region,
a.country,
a.adjusted_user_key,
max(case when date_trunc('quarter',a.date) = date_trunc('quarter',b.signup_date) then 1 else 0 end) as new_account_flag,
sum(a.tvms) as tvms,
sum(a.reg_tvms) as reg_tvms,
sum(a.loggedin_tvms) as loggedin_tvms
from
SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK a
left join
    client_user_mapping b
on
    a.adjusted_user_key = b.user_key
where a.adjusted_user_key is not null
group by all
),

quarterly_final as (
select
date,
  'quarterly' as date_type,
  region_type,
  region,
  country,
  case when new_account_flag = 1 then 'new' else 'returning' end as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from quarterly_accounts
  group by all
  
union
  
select
date,
  'quarterly' as date_type,
  region_type,
  region,
  country,
  'ttl' as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from quarterly_accounts
  group by all
),

weekly_accounts as (
select
date_trunc('week',a.date) as date,
a.region_type,
a.region,
a.country,
a.adjusted_user_key,
max(case when date_trunc('week',a.date) = date_trunc('week',b.signup_date) then 1 else 0 end) as new_account_flag,
sum(a.tvms) as tvms,
sum(a.reg_tvms) as reg_tvms,
sum(a.loggedin_tvms) as loggedin_tvms
from
SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK a
left join
    client_user_mapping b
on
    a.adjusted_user_key = b.user_key
where a.adjusted_user_key is not null
group by all
),

weekly_final as (
select
date,
  'weekly' as date_type,
  region_type,
  region,
  country,
  case when new_account_flag = 1 then 'new' else 'returning' end as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from weekly_accounts
  group by all
  
  union
  
select
date,
  'weekly' as date_type,
  region_type,
  region,
  country,
  'ttl' as account_tenure,
 count(distinct adjusted_user_key) as num_accounts,
sum(tvms) as tvms,
sum(reg_tvms) as reg_tvms,
sum(loggedin_tvms) as loggedin_tvms
  from weekly_accounts
  group by all
),
---------------------------
first_time_account_seen as (
select
  region_type,
  region,
  country,
  adjusted_user_key,
  min(date) as min_date
from
    daily_accounts
group by all
),

account_staging as (
select
  a.region_type,
  a.region,
  a.country,
  a.adjusted_user_key,
  a.min_date,
  max(b.new_account_flag) as tenure_flag
from
  first_time_account_seen a
join (select
  a.region_type,
  a.region,
  a.country,
  a.adjusted_user_key,
  a.date,
  max(a.new_account_flag) as new_account_flag from daily_accounts a group by all) b on a.adjusted_user_key = b.adjusted_user_key and a.min_date = b.date
  group by all
),
accounts as (
select min_date as date,
  region_type,
  region,
  country,
  'ttl' as account_tenure,
  count(distinct adjusted_user_key) as num_accounts
from
  account_staging
  group by all
  
union
  
select min_date as date,
  region_type,
  region,
  country,
  case when tenure_flag = 1 then 'new' else 'returning' end as account_tenure,
  count(distinct adjusted_user_key) as num_accounts
from
  account_staging
  group by all
),


dates as (
select
  date
  from accounts
  group by all
),

dim_base as (
select
  a.region_type,
  a.region,
  a.country,
  a.account_tenure
from
    accounts a
  group by all
),

alltime_base as (
select
a.date,
b.region_type,
b.region,
b.country,
b.account_tenure
from
dates a
cross join
dim_base b
group by all
),
alltime_accounts_final as (
select a.date,
  'all-time' as date_type,
  a.region_type,
  a.region,
  a.country,
  a.account_tenure,
  sum(SUM(b.num_accounts)) OVER (PARTITION BY a.region_type,a.region,a.country,a.account_tenure ORDER BY a.date ASC rows between unbounded preceding and current row) as num_accounts
from
  alltime_base a
full join
  accounts b on a.region_type = b.region_type and a.region = b.region and a.account_tenure = b.account_tenure and a.country = b.country and a.date = b.date
  group by all
),
alltime_final as (
select
*,
case when sum(
ifnull(num_accounts,0)) > 0 then 0 else 1 end as ind
        from alltime_accounts_final
        group by all
        qualify ind = 0
        ),
        final as (
select
a.date,
a.date_type,
a.region_type,
a.region,
a.country,
a.account_tenure,
a.num_accounts,
lag(a.num_accounts,365) over (partition by a.date_type,a.region,a.region_type,a.account_tenure order by a.date asc) as prev_period_num_accounts
from
daily_final a
group by 1,2,3,4,5,6,7

union
select
b.date,
b.date_type,
b.region_type,
b.region,
b.country,
b.account_tenure,
b.num_accounts,
lag(b.num_accounts,12) over (partition by b.date_type,b.region,b.region_type,b.account_tenure order by b.date asc) as prev_period_num_accounts
from
monthly_final b
group by 1,2,3,4,5,6,7
union
select
c.date,
c.date_type,
c.region_type,
c.region,
c.country,
c.account_tenure,
c.num_accounts,
lag(c.num_accounts,1) over (partition by c.date_type,c.region,c.region_type,c.account_tenure order by c.date asc) as prev_period_num_accounts
from
yearly_final c
group by 1,2,3,4,5,6,7

union
select
d.date,
d.date_type,
d.region_type,
d.region,
d.country,
d.account_tenure,
d.num_accounts,
lag(d.num_accounts,4) over (partition by d.date_type,d.region,d.region_type,d.account_tenure order by d.date asc) as prev_period_num_accounts
from
quarterly_final d
group by 1,2,3,4,5,6,7

union
select
f.date,
f.date_type,
f.region_type,
f.region,
f.country,
f.account_tenure,
f.num_accounts,
lag(f.num_accounts,4) over (partition by f.date_type,f.region,f.region_type,f.account_tenure order by f.date asc) as prev_period_num_accounts
from
weekly_final f
group by 1,2,3,4,5,6,7

union

select 
e.date,
e.date_type,
  e.region_type,
  e.region,
  e.country,
  e.account_tenure,
  e.num_accounts,
  lag(e.num_accounts,365) over (partition by e.date_type,e.region,e.region_type,e.account_tenure order by e.date asc) as prev_period_num_accounts
  from alltime_final e
  group by 1,2,3,4,5,6,7
)
select *
from final
WHERE 
 (date_type = 'daily' and date >= (current_date() - 8)) 
    or
 (date_type = 'monthly' and date >= dateadd('month',-1,date_trunc('month',current_date()))) 
    or 
 (date_type = 'yearly' and date >= dateadd('year',-1,date_trunc('year',current_date())))
    or
 (date_type = 'weekly' and date >= dateadd('week',-1,date_trunc('week',current_date())))
    or
 (date_type = 'quarterly' and date >= dateadd('quarter',-1,date_trunc('quarter',current_date())))
    or
 (date_type = 'all-time' and date >= (current_date() - 8))
;
return 'REGISTRATION_DASHBOARD_USER_FINAL_AGG';
end;

$$