-- Database initialization script
-- This script runs when the PostgreSQL container starts

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA marts TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO postgres;

-- Create raw telegram messages table
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    id SERIAL PRIMARY KEY,
    message_id BIGINT,
    channel_name VARCHAR(255),
    channel_id BIGINT,
    sender_id BIGINT,
    sender_username VARCHAR(255),
    message_text TEXT,
    message_date TIMESTAMP,
    has_media BOOLEAN,
    media_type VARCHAR(50),
    views INTEGER,
    forwards INTEGER,
    replies INTEGER,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel_id ON raw.telegram_messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_message_date ON raw.telegram_messages(message_date);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_sender_id ON raw.telegram_messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_raw_data ON raw.telegram_messages USING GIN(raw_data);

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'Database initialization completed successfully';
END $$; 