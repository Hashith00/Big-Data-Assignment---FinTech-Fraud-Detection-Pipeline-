-- Create the fraud_transactions table
CREATE TABLE IF NOT EXISTS fraud_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    user_id VARCHAR(255),
    event_time TIMESTAMP,
    merchant_category VARCHAR(255),
    amount NUMERIC(12, 2),
    location VARCHAR(255),
    fraud_type VARCHAR(100),
    fraud_reason TEXT,
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fraud_user_id ON fraud_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_event_time ON fraud_transactions(event_time);
CREATE INDEX IF NOT EXISTS idx_fraud_type ON fraud_transactions(fraud_type);
