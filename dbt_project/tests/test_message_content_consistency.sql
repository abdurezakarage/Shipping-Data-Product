-- Custom test: Message content consistency
-- This test ensures that message content and metadata are consistent

SELECT 
    message_key,
    message_text,
    has_text,
    message_length,
    word_count,
    message_type,
    has_media,
    media_type
FROM {{ ref('fct_messages') }}
WHERE 
    -- Test 1: If has_text is true, message_text should not be null or empty
    (has_text = TRUE AND (message_text IS NULL OR message_text = ''))
    
    -- Test 2: If has_text is false, message_text should be null or empty
    OR (has_text = FALSE AND message_text IS NOT NULL AND message_text != '')
    
    -- Test 3: Message length should match actual text length
    OR (message_text IS NOT NULL AND message_length != LENGTH(message_text))
    
    -- Test 4: Word count should be reasonable (not negative)
    OR word_count < 0
    
    -- Test 5: Message type should be consistent with content
    OR (message_type = 'text' AND NOT has_text)
    OR (message_type = 'media' AND NOT has_media)
    
    -- Test 6: If has_media is true, media_type should not be null
    OR (has_media = TRUE AND media_type IS NULL)
    
    -- Test 7: Message length category should be consistent
    OR (message_length_category = 'empty' AND message_length > 0)
    OR (message_length_category = 'short' AND (message_length <= 0 OR message_length > 50))
    OR (message_length_category = 'medium' AND (message_length <= 50 OR message_length > 100))
    OR (message_length_category = 'long' AND message_length <= 100)
    
    -- Test 8: Message detail level should be consistent
    OR (message_detail_level = 'empty' AND word_count > 0)
    OR (message_detail_level = 'brief' AND (word_count <= 0 OR word_count > 10))
    OR (message_detail_level = 'moderate' AND (word_count <= 10 OR word_count > 20))
    OR (message_detail_level = 'detailed' AND word_count <= 20) 