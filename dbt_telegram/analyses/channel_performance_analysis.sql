-- Channel Performance Analysis
-- This analysis provides insights into channel engagement and content performance

with channel_metrics as (
    select
        c.channel_username,
        c.channel_title,
        c.channel_category,
        c.total_messages_scraped,
        
        -- Message counts by content type
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
        
        -- Time-based metrics
        count(distinct f.date_key) as active_days,
        min(f.message_date) as first_message_date,
        max(f.message_date) as last_message_date
        
    from {{ ref('dim_channels') }} c
    left join {{ ref('fct_messages') }} f on c.channel_key = f.channel_key
    group by 1, 2, 3, 4
),

channel_rankings as (
    select
        *,
        -- Rankings
        rank() over (order by avg_views desc) as views_rank,
        rank() over (order by avg_forward_rate desc) as forward_rate_rank,
        rank() over (order by avg_reply_rate desc) as reply_rate_rank,
        rank() over (order by total_messages_scraped desc) as activity_rank
        
    from channel_metrics
)

select 
    channel_username,
    channel_title,
    channel_category,
    total_messages_scraped,
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
    active_days,
    first_message_date,
    last_message_date,
    views_rank,
    forward_rate_rank,
    reply_rate_rank,
    activity_rank
    
from channel_rankings
order by avg_views desc 