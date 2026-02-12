import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}")
SCHEMA = 'public'

tickers = {
    'SPY': 'SP500_ETF',
    'QQQ': 'Nasdaq100_ETF',
    'AAPL': 'Apple',
    'MSFT': 'Microsoft',
    'GOOGL': 'Alphabet',
    'AMZN': 'Amazon',
    'TSLA': 'Tesla',
    'NVDA': 'NVIDIA',
    'BTC-USD': 'Bitcoin'
}

def upsert_dim_tickers(engine, df: pd.DataFrame):
    table = 'dim_tickers'
    rows = df.to_dict(orient="records")

    stmt = insert(pd.io.sql.SQLTable(table, pd.io.sql.pandasSQL_builder(engine), frame=df, index=False).table).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol'],
        set_={
            'name': stmt.excluded.name,
            'market': stmt.excluded.market,
            'first_date': stmt.excluded.first_date,
            'last_date': stmt.excluded.last_date,
            'avg_volume': stmt.excluded.avg_volume,
        }
    )

    with engine.begin() as conn:
        conn.execute(stmt)


def upsert_fact_ohlcv(engine, df: pd.DataFrame):
    table = 'fact_ohlcv'
    rows = df.to_dict(orient="records")

    stmt = insert(pd.io.sql.SQLTable(table, pd.io.sql.pandasSQL_builder(engine), frame=df, index=False).table).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol', 'date'],
        set_={
            'open_price': stmt.excluded.open_price,
            'high_price': stmt.excluded.high_price,
            'low_price': stmt.excluded.low_price,
            'close_price': stmt.excluded.close_price,
            'volume': stmt.excluded.volume,
            'adj_close': stmt.excluded.adj_close,
        }
    )

    with engine.begin() as conn:
        conn.execute(stmt)

for symbol, name in tickers.items():
    print(f"Fetching data for {symbol}...")
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(start="2020-01-01", end=datetime.now().strftime('%Y-%m-%d'))

        if hist.empty:
            print(f"No data found for {symbol}. Skipping.")
            continue

        df = hist.reset_index()
        df['date'] = pd.to_datetime(df['Date']).dt.date
        df['symbol'] = symbol
        df['market'] = name.split()[0] if len(name.split()) > 1 else symbol

        fact_df = df[['date', 'symbol', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
        fact_df.columns = ['date', 'symbol', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
        fact_df['adj_close'] = fact_df['close_price']  # Placeholder tqt
        fact_df = fact_df.dropna()

        # UPSERT pour DimTickers
        ticker_info = pd.DataFrame([{
            'symbol': symbol,
            'name': name,
            'market': df['market'].iloc[0],
            'first_date': fact_df['date'].min(),
            'last_date': fact_df['date'].max(),
            'avg_volume': int(fact_df['volume'].mean())
        }])
        
        upsert_dim_tickers(engine, ticker_info)
        upsert_fact_ohlcv(engine, fact_df)

        with engine.begin() as conn:
            c1 = conn.execute(text('SELECT COUNT(*) FROM "fact_ohlcv" WHERE symbol = :symbol'), {"symbol": symbol}).scalar()
            c2 = conn.execute(text('SELECT COUNT(*) FROM "dim_tickers" WHERE symbol = :symbol'), {"symbol": symbol}).scalar()
        print("DB counts:", symbol, c2, c1)
        
        print(f"Bien chargé {len(fact_df)} lignes pour {symbol}. ({fact_df['date'].min()} to {fact_df['date'].max()})")

    except Exception as e:
        print(f"Erreur lors du chargement de {symbol}: {e}")

stats = pd.read_sql('SELECT symbol, COUNT(*) as count, MIN(date) as first_date, MAX(date) as last_date FROM "fact_ohlcv" GROUP BY symbol', engine)
print(stats)