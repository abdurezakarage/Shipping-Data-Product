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
        channel_name,
        channel_id,
        sender_id,
        sender_username,
        
        -- Message content
        message_text,
        
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
        has_media,
        media_type,
        
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
            WHEN (message_text IS NOT NULL AND message_text != '') 
            THEN 'text'
            ELSE 'other'
        END as message_type,
        
        loaded_at
        
    FROM source
    WHERE message_id IS NOT NULL  -- Filter out invalid messages
)

SELECT * FROM cleaned 