/** We want to create a daily report to track:

Total unique sessions
Columns: dateTrunc(daily),unique sessions **/

with session_info as (
select 
	session_id
	,min(event_timestamp) as start_time_session
	,max(event_timestamp) as end_time_session
    ,timediff(second,start_time_session,end_time_session) as session_length_seconds
from vk_data.events.website_activity
	group by session_id),
/** I did group by(142ms) to resolve the count distinct(323ms) **/
daily_session_avg as(
select
	date_trunc("day",start_time_session) as date_daily
	,count(session_id) as total_unique_sessions
from session_info
	group by date_daily),
/** I completed another group by and used only two columns to keep things efficent, 140ms vs count distinct of 378ms **/
/** The average length of sessions in seconds
Columns: dateTrunc(), avg(session_time(seconds))
Session ID, start_session, end_session,session_time(seconds) **/
daily_session_length_avg as(
select
	date_trunc("day",start_time_session) as date_daily
	,avg(session_length_seconds) as average_session_length
    /**,sum(session_length_seconds) / count(session_id) <--- decreased my query by 10ms, 
    not worth keeping in the query so I went with the average function **/
from session_info
	group by date_daily),
/** I completed another group by and used only two columns to keep things efficent, 140ms vs count distinct of 378ms **/
/**The ID of the recipe that was most viewed 
Columns:
dateTrunc(),most_viewed_recipe **/
session_recipe as (
select 
	date_trunc("day",event_timestamp) as date_daily
    ,parse_json(event_details):recipe_id::varchar as recipe_id
    ,count(*) as recipe_count
    ,row_number() OVER(PARTITION BY date_daily ORDER BY recipe_count desc ) as row_number
    /** NTS: Rank will give us multi-same place ranking : 1 2 2 3. Row number is in order of appearance **/
from vk_data.events.website_activity
  	where recipe_id is not null 
    group by 1,2
 	qualify row_number = 1),
/**The average number of searches completed before displaying a recipe
Columns: dateTrunc( earliest view reciep),avg(search_completed)
Session_ID, earliest view reciepe, count searches before earliest recipe **/
session_event_type_count as (
select
	session_id
    ,event_timestamp
    ,iff(parse_json(event_details):event::varchar = 'search'=TRUE,1,0) as search_count
   	,iff(parse_json(event_details):event::varchar = 'view_recipe'=TRUE,1,0) as view_recipe_count
from vk_data.events.website_activity),
session_search_sum as(
select
	session_id
    ,min(event_timestamp) as start_time_session
    ,sum(search_count) as session_search_count
    ,sum(view_recipe_count) as session_view_recipe_count
from session_event_type_count
	group by 1),
daily_avg_search as (
select
	date_trunc("day",start_time_session) as date_daily
    ,avg(session_search_count) as daily_avg_search_count
from session_search_sum
	group by 1),
daily_metrics as (
select
	daily_session_avg.date_daily
	,total_unique_sessions
    ,average_session_length as avg_session_length_seconds
    ,recipe_id as highest_viewed_recipe
    ,daily_avg_search_count as avg_search_count_to_view_recipe
from daily_session_avg
left join daily_session_length_avg
	on daily_session_avg.date_daily = daily_session_length_avg.date_daily
left join session_recipe
	on daily_session_avg.date_daily =session_recipe.date_daily
left join daily_avg_search
	on daily_session_avg.date_daily =daily_avg_search.date_daily
)
select * from daily_metrics






