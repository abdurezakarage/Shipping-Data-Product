{{ config(materialized='table') }}

with detections as (
    select
        d.message_id,
        d.image_filename,
        d.detected_object_class,
        d.confidence_score
    from {{ source('raw', 'image_detections') }} d
),

messages as (
    select
        message_key,
        message_id,
        channel_name,
        message_timestamp
    from {{ ref('fct_messages') }}
)

select
    m.message_key,
    d.image_filename,
    d.detected_object_class,
    d.confidence_score,
    m.channel_name,
    m.message_timestamp
from detections d
left join messages m
    on d.message_id = m.message_id