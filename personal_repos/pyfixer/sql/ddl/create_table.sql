DROP TABLE IF EXISTS forex_rates.fx_price_index;

CREATE TABLE forex_rates.fx_price_index (
    currency VARCHAR(6),
    rate DECIMAL(19,4),
    base VARCHAR(6),
    created_at TIMESTAMP,
    inserted_at TIMESTAMP
)