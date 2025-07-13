{{
  config(
    materialized='view',
    tags=['staging', 'telegram']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
),

cleaned AS (
    SELECT
        -- Primary key
        id as message_key,
        
        -- Message identifiers
        message_id,
        channel_title,
        channel_username,
        
        -- Message content
        message_text,
        CASE 
            WHEN message_text IS NULL OR message_text = '' THEN FALSE 
            ELSE TRUE 
        END as has_text,
        
        -- Date and time
        CASE 
            WHEN message_date IS NOT NULL 
            THEN message_date::timestamp 
            ELSE NULL 
        END as message_timestamp,
        
        -- Extract date components for analysis
        CASE 
            WHEN message_date IS NOT NULL 
            THEN DATE(message_date::timestamp) 
            ELSE NULL 
        END as message_date,
        
        EXTRACT(YEAR FROM message_date::timestamp) as message_year,
        EXTRACT(MONTH FROM message_date::timestamp) as message_month,
        EXTRACT(DAY FROM message_date::timestamp) as message_day,
        EXTRACT(HOUR FROM message_date::timestamp) as message_hour,
        EXTRACT(DOW FROM message_date::timestamp) as message_day_of_week,
        
        -- Media information
        CASE 
            WHEN media_path IS NOT NULL AND media_path != '' THEN TRUE 
            ELSE FALSE 
        END as has_media,
        
        CASE 
            WHEN media_path LIKE '%.jpg' OR media_path LIKE '%.jpeg' OR media_path LIKE '%.png' THEN 'photo'
            WHEN media_path LIKE '%.mp4' OR media_path LIKE '%.avi' OR media_path LIKE '%.mov' THEN 'video'
            WHEN media_path LIKE '%.mp3' OR media_path LIKE '%.wav' THEN 'audio'
            WHEN media_path IS NOT NULL AND media_path != '' THEN 'other'
            ELSE NULL 
        END as media_type,
        
        media_path,
        
        -- Calculated fields
        CASE 
            WHEN message_text IS NOT NULL 
            THEN LENGTH(message_text) 
            ELSE 0 
        END as message_length,
        
        CASE 
            WHEN message_text IS NOT NULL 
            THEN ARRAY_LENGTH(STRING_TO_ARRAY(message_text, ' '), 1) 
            ELSE 0 
        END as word_count,
        
        -- Message type classification
        CASE 
            WHEN has_media = TRUE AND media_type IS NOT NULL 
            THEN 'media'
            WHEN has_text = TRUE 
            THEN 'text'
            ELSE 'other'
        END as message_type,
        
        -- Channel classification
        CASE 
            WHEN channel_username LIKE '%CheMed%' THEN 'CheMed'
            WHEN channel_username LIKE '%lobelia%' THEN 'Lobelia'
            WHEN channel_username LIKE '%tikvah%' THEN 'Tikvah'
            ELSE 'Other'
        END as channel_category,
        
        loaded_at
        
    FROM source
    WHERE message_id IS NOT NULL  -- Filter out invalid messages
)

SELECT * FROM cleaned 