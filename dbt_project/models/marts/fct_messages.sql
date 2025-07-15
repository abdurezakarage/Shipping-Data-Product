{{
  config(
    materialized='table',
    tags=['marts', 'facts']
  )
}}

WITH messages AS (
    SELECT
        message_key,
        message_id,
        channel_name,
        channel_id,
        message_date,
        message_text,
        message_timestamp,
        message_year,
        message_month,
        message_day,
        message_hour,
        message_day_of_week,
        has_media,
        media_type,
        message_length,
        word_count,
        message_type,
        loaded_at
    FROM {{ ref('stg_telegram_messages') }}
),

channels AS (
    SELECT channel_key, channel_id
    FROM {{ ref('dim_channels') }}
),

dates AS (
    SELECT date_key, year, month, day, day_of_week, quarter, is_weekend
    FROM {{ ref('dim_dates') }}
)

SELECT
    -- Primary key
    m.message_key,
    
    -- Foreign keys to dimension tables
    c.channel_key,
    d.date_key,
    
    -- Message identifiers
    m.message_id,
    m.channel_name,
    m.channel_id,
    
    -- Message content
    m.message_text,
    m.message_length,
    m.word_count,
    m.message_type,
    
    -- Time attributes
    m.message_timestamp,
    m.message_hour,
    d.is_weekend,
    
    -- Media information
    m.has_media,
    m.media_type,
    
    -- Message quality indicators
    CASE 
        WHEN m.message_length > 100 THEN 'long'
        WHEN m.message_length > 50 THEN 'medium'
        WHEN m.message_length > 0 THEN 'short'
        ELSE 'empty'
    END as message_length_category,
    
    CASE 
        WHEN m.word_count > 20 THEN 'detailed'
        WHEN m.word_count > 10 THEN 'moderate'
        WHEN m.word_count > 0 THEN 'brief'
        ELSE 'empty'
    END as message_detail_level,
    
    -- Content analysis
    CASE 
        WHEN m.message_text ILIKE '%medic%' OR m.message_text ILIKE '%drug%' OR m.message_text ILIKE '%pharma%' 
        THEN TRUE 
        ELSE FALSE 
    END as contains_medical_content,
    
    CASE 
        WHEN m.message_text ILIKE '%order%' OR m.message_text ILIKE '%buy%' OR m.message_text ILIKE '%purchase%' 
        THEN TRUE 
        ELSE FALSE 
    END as contains_purchase_intent,
    
    CASE 
        WHEN m.message_text ILIKE '%delivery%' OR m.message_text ILIKE '%ship%' OR m.message_text ILIKE '%deliver%' 
        THEN TRUE 
        ELSE FALSE 
    END as contains_delivery_content,
    
    m.loaded_at,
    CURRENT_TIMESTAMP as dbt_updated_at

FROM messages m
LEFT JOIN channels c ON m.channel_id = c.channel_id
LEFT JOIN dates d ON m.message_date = d.date_key 