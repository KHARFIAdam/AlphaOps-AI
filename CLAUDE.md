# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**AlphaOps AI** is a production-oriented financial AI system combining data engineering, an LLM-powered NL-to-SQL agent, ML forecasting (LSTM), and autonomous MLOps workflows. The current implemented layers are data ingestion and the NL2SQL agent; the ML forecasting and autonomous agent layers are planned/in progress.

## Environment Setup

Copy `.env.example` to `.env` and fill in credentials:
```
PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD   # financial data DB
AIRFLOW_PGDATABASE, AIRFLOW_PGUSER, AIRFLOW_PGPASSWORD  # Airflow metadata DB
MISTRAL_API_KEY                                   # Mistral/Codestral LLM
```

Activate the virtual environment:
```bash
source .venv/Scripts/activate   # Windows Git Bash
```

Install dependencies:
```bash
pip install -r requirements.txt
```

## Infrastructure

Two PostgreSQL instances are run via Docker Compose:
- `postgres_data` → financial data DB, exposed on **port 5433**
- `postgres_airflow` → Airflow metadata DB, exposed on **port 5434**
- Airflow webserver at **http://localhost:8081** (admin/admin)

```bash
docker compose up -d
```

The entire project directory is mounted into Airflow containers at `/opt/airflow`.

## Key Commands

**Test DB connection:**
```bash
python test_co.py
```

**Initialize the star schema (destructive — drops and recreates all tables):**
```bash
python data/create_stocks_schema.py
```

**Run incremental stock ingestion manually:**
```bash
python data/fetch_live_stocks.py
```

**Run the NL2SQL agent (hardcoded test query):**
```bash
cd src && python agent.py
```

## Architecture

### Database Schema (Star Schema)

Three tables in `public` schema:
- `dim_tickers` — ticker metadata (symbol PK, name, market, sector, date range, avg_volume)
- `dimtime` — date dimension (date PK, year/month/day/quarter/day_of_week, flags), seeded 2020–2026
- `fact_ohlcv` — OHLCV price data (symbol+date composite PK), range-partitioned by year (yearly partition tables `fact_ohlcv_YYYY`), with a computed `volatility` column

Tracked tickers: SPY, QQQ, AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, BTC-USD.

### NL2SQL Agent (`src/`)

LangGraph-based pipeline with four sequential nodes:

```
get_schema → generate_sql → validate_sql → execute_sql
```

- `get_schema`: fetches live table DDL from the DB via LangChain `SQLDatabase`
- `generate_sql`: sends schema + user question to **Mistral Codestral** with a schema-grounded prompt (French prompts, handles non-trading days via `MAX(date) <= target_date`)
- `validate_sql` (`src/sql_validator.py`): parses with `sqlglot`; enforces SELECT-only, allowed tables only (`dim_tickers`, `dimtime`, `fact_ohlcv`), blocks dangerous patterns
- `execute_sql`: runs query against DB; skips execution if validation fails

`src/mistral-llm.py` is a standalone prototype of the LLM chain without the agent graph.

Note: `src/agent.py` currently hardcodes the DB connection string (not using `.env`) — use `.env`-based credentials for any new connections.

### Data Ingestion (`data/`)

`fetch_live_stocks.py` implements incremental loading:
1. Checks `MAX(date)` per symbol in `fact_ohlcv` to determine start date
2. Downloads via `yfinance` from last loaded date to today
3. Upserts to `dim_tickers` and `fact_ohlcv` using `ON CONFLICT DO UPDATE`
4. Auto-creates year partitions as needed

### Airflow DAG (`dags/daily_ingestion.py`)

- DAG ID: `daily_stock_ingestion`
- Schedule: `0 18 * * 1-5` (weekdays at 18:00)
- Calls `run_daily_batch()` from `data/fetch_live_stocks.py`
- Retries: 2 attempts with 5-minute delay
