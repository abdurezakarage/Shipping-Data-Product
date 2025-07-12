-- Custom test to ensure message content consistency
-- This test should return 0 rows to pass

select 
    message_key,
    has_text,
    has_media,
    message_length
from {{ ref('fct_messages') }}
where 
    -- If message has text, length should be greater than 0
    (has_text = true and message_length <= 0)
    or
    -- If message has no text, length should be 0
    (has_text = false and message_length > 0)
    or
    -- Message length should not be negative
    message_length < 0 