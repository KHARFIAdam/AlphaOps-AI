import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT")
)

engine = create_engine(f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}")
conn.autocommit = True
cur = conn.cursor()

# DimTickers
cur.execute("""
DROP TABLE IF EXISTS dim_tickers CASCADE;
CREATE TABLE dim_tickers (
    symbol VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    market VARCHAR(20) NOT NULL,  -- 'NASDAQ', 'SP500_ETF', 'Crypto'
    sector VARCHAR(50),           -- 'Tech', 'Finance' (optionnel via yf.info)
    first_date DATE,
    last_date DATE,
    avg_volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

# DimTime
cur.execute("""
DROP TABLE IF EXISTS dim_time CASCADE;
CREATE TABLE dim_time (
    date DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_month_end BOOLEAN NOT NULL
);
-- Seed DimTime de 2020 à 2026
INSERT INTO DimTime SELECT
    d::date as date,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    EXTRACT(DAY FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(ISODOW FROM d)::INT - 1,  -- 0=Monday, 6=Sunday
    EXTRACT(ISODOW FROM d) IN (6, 7),
    (d::date = (date_trunc('month', d::date) + interval '1 month - 1 day')::date)
FROM generate_series('2020-01-01'::date, '2026-12-31'::date, '1 day'::interval) d;
""")

# FactOHLCV
cur.execute("""
DROP TABLE IF EXISTS fact_ohlcv CASCADE;
CREATE TABLE fact_ohlcv (
    symbol VARCHAR(20) NOT NULL REFERENCES dim_tickers(symbol),
    date DATE NOT NULL REFERENCES dim_time(date),
    open_price DECIMAL(12,4) NOT NULL,
    high_price DECIMAL(12,4) NOT NULL,
    low_price DECIMAL(12,4) NOT NULL,
    close_price DECIMAL(12,4) NOT NULL,
    volume BIGINT NOT NULL,
    adj_close DECIMAL(12,4),
    volatility DECIMAL(8,4) GENERATED ALWAYS AS ((high_price - low_price) / close_price) STORED,  -- auto-computed
    PRIMARY KEY (symbol, date),
    CHECK (open_price >= 0), CHECK (volume >= 0)
) PARTITION BY RANGE (date);  -- Partition future-proof (millions rows)

-- Indexes pour NL2SQL
CREATE INDEX idx_fact_symbol_date ON fact_ohlcv (symbol, date);
CREATE INDEX idx_fact_date_symbol ON fact_ohlcv (date, symbol);
CREATE INDEX idx_fact_volume ON fact_ohlcv (volume DESC);
CREATE INDEX idx_fact_close ON fact_ohlcv (close_price DESC);
            
-- Exemple de partitionnement (année)
CREATE TABLE fact_ohlcv_2025 PARTITION OF fact_ohlcv FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
""")

cur.close()
conn.close()
print("Schema créé !")