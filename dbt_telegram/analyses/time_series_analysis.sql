-- Time Series Analysis
-- This analysis provides insights into message activity patterns over time

with daily_metrics as (
    select
        d.date_key,
        d.day_name,
        d.month_name,
        d.is_weekend,
        d.is_weekday,
        
        -- Message counts
        count(f.message_key) as total_messages,
        count(case when f.content_type = 'image' then 1 end) as image_messages,
        count(case when f.content_type = 'document' then 1 end) as document_messages,
        count(case when f.content_type = 'text_only' then 1 end) as text_only_messages,
        
        -- Engagement metrics
        avg(f.views) as avg_views,
        avg(f.forwards) as avg_forwards,
        avg(f.replies) as avg_replies,
        avg(f.forward_rate_percent) as avg_forward_rate,
        avg(f.reply_rate_percent) as avg_reply_rate,
        
        -- Content metrics
        avg(f.message_length) as avg_message_length,
        count(case when f.has_media = true then 1 end) as media_messages,
        
        -- Channel activity
        count(distinct f.channel_key) as active_channels
        
    from {{ ref('dim_dates') }} d
    left join {{ ref('fct_messages') }} f on d.date_key = f.date_key
    group by 1, 2, 3, 4, 5
),

weekly_metrics as (
    select
        date_trunc('week', date_key) as week_start,
        sum(total_messages) as weekly_messages,
        avg(avg_views) as weekly_avg_views,
        avg(avg_forward_rate) as weekly_avg_forward_rate,
        avg(avg_reply_rate) as weekly_avg_reply_rate,
        count(distinct date_key) as days_with_activity
        
    from daily_metrics
    where total_messages > 0
    group by 1
),

monthly_metrics as (
    select
        date_trunc('month', date_key) as month_start,
        sum(total_messages) as monthly_messages,
        avg(avg_views) as monthly_avg_views,
        avg(avg_forward_rate) as monthly_avg_forward_rate,
        avg(avg_reply_rate) as monthly_avg_reply_rate,
        count(distinct date_key) as days_with_activity
        
    from daily_metrics
    where total_messages > 0
    group by 1
)

select 
    'daily' as granularity,
    date_key as time_period,
    day_name,
    is_weekend,
    total_messages,
    image_messages,
    document_messages,
    text_only_messages,
    round(avg_views, 2) as avg_views,
    round(avg_forwards, 2) as avg_forwards,
    round(avg_replies, 2) as avg_replies,
    round(avg_forward_rate, 2) as avg_forward_rate_percent,
    round(avg_reply_rate, 2) as avg_reply_rate_percent,
    round(avg_message_length, 2) as avg_message_length,
    media_messages,
    active_channels
    
from daily_metrics
where total_messages > 0

union all

select 
    'weekly' as granularity,
    week_start as time_period,
    'Week' as day_name,
    null as is_weekend,
    weekly_messages as total_messages,
    null as image_messages,
    null as document_messages,
    null as text_only_messages,
    round(weekly_avg_views, 2) as avg_views,
    null as avg_forwards,
    null as avg_replies,
    round(weekly_avg_forward_rate, 2) as avg_forward_rate_percent,
    round(weekly_avg_reply_rate, 2) as avg_reply_rate_percent,
    null as avg_message_length,
    null as media_messages,
    null as active_channels
    
from weekly_metrics

union all

select 
    'monthly' as granularity,
    month_start as time_period,
    'Month' as day_name,
    null as is_weekend,
    monthly_messages as total_messages,
    null as image_messages,
    null as document_messages,
    null as text_only_messages,
    round(monthly_avg_views, 2) as avg_views,
    null as avg_forwards,
    null as avg_replies,
    round(monthly_avg_forward_rate, 2) as avg_forward_rate_percent,
    round(monthly_avg_reply_rate, 2) as avg_reply_rate_percent,
    null as avg_message_length,
    null as media_messages,
    null as active_channels
    
from monthly_metrics

order by granularity, time_period 