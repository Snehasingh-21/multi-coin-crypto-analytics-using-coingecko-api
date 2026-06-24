# 📊 Multi-Coin Crypto Analytics Using CoinGecko API

End-to-end pipeline (Airflow • Snowflake • dbt • Prophet • Preset • Docker).

**Course:** DATA 226  
**Technologies:** Apache Airflow • Snowflake • dbt • Prophet • Preset BI • Docker

## 📑 Table of Contents
- [Project Objective](#project-objective)
- [Data Sources](#data-sources)
- [Design Decisions](#design-decisions)
- [System Architecture](#system-architecture)
- [Pipeline Components](#pipeline-components)
- [Data Quality & Validation](#data-quality-and-validation)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Key Features](#key-features)
- [Assumptions & Limitations](#assumptions--limitations)
- [Future Enhancements](#future-enhancements)
- [Conclusion](#conclusion)


---

 ## 🎯 Project Objective

The goal of this project is to build an end-to-end automated analytics pipeline that processes, transforms, forecasts, and visualizes cryptocurrency market data for multiple assets.

The pipeline:
- Ingests real-time and historical crypto market data
- Performs scalable ELT transformations
- Generates machine learning–based forecasts
- Produces technical alerts
- Visualizes insights through interactive dashboards

Supported crypto currencies:
Bitcoin, Ethereum, Binance Coin, Solana, and Cardano

---

## 📥 Data Sources

- CoinGecko API
  - Daily market metrics
  - Hourly intraday prices
  - OHLC (Open, High, Low, Close) data

Ingestion Frequency:
- Daily ETL → historical analysis and indicators
- Hourly ETL → near real-time monitoring and alerts

CoinGecko provides normalized, exchange-agnostic crypto pricing suitable for analytical use cases.

---

## 🧠 Design Decisions

- Apache Airflow orchestrates multi-DAG workflows and scheduling.
- Snowflake serves as the cloud data warehouse with RAW and ANALYTICS separation.
- dbt enables version-controlled ELT, testing, and snapshots.
- Prophet is used for time-series forecasting due to robustness to missing data and seasonality.
- Preset BI provides SQL-native, production-grade dashboards.

The architecture follows modern ELT best practices.

---

## 🏗️ System Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                           CRYPTO DATA PLATFORM                                │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐
│   Docker     │
│ Initialize   │
│ Airflow      │
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│  Starts Airflow  │
└──────┬───────────┘
       │
       ├───────────────────────────────────────────────┐
       │                                               │
       ▼                                               ▼
┌──────────────────┐                          ┌──────────────────┐
│     Airflow      │                          │     Airflow      │
│   ETL DAG        │                          │   Hourly DAG     │
└──────┬───────────┘                          └──────┬───────────┘
       │                                               │
       │                                               ▼
       │                                    ┌──────────────────────────────┐
       │                                    │ Transform data and load      │
       │                                    │ into RAW schema              │
       │                                    └──────────┬───────────────────┘
       │                                               │
       ▼                                               ▼
┌──────────────────────────┐                 ┌──────────────────────────┐
│ Triggers ELT DAG         │                 │        Snowflake         │
└──────────┬───────────────┘                 │        RAW Schema        │
           │                                └──────────┬───────────────-┘
           │                                           │
           ▼                                           ▼
┌──────────────────┐                         ┌──────────────────────────┐
│     Airflow      │◀────────────────────────│ dbt reads RAW data       │
│   DBT ELT DAG    │                         │                          │
└──────┬───────────┘                         └──────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ dbt run • dbt test • dbt snapshot                                    │
│ Triggers Forecast DAG                                                │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────┐
│     Airflow      │
│ Forecast DAG     │
│ (Prophet)        │
└──────┬───────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Loads forecasts → ANALYTICS.CRYPTO_FORECAST_FINAL                    │
│ Triggers Alerts DAG                                                  │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────┐
│     Airflow      │
│ Crypto Alerts    │
│ DAG              │
└──────┬───────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Generates alert indicators                                           │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────┐
│   Snowflake      │
│ ANALYTICS        │
└──────┬───────────┘
       │
       ▼
┌──────────────────────────────┐
│ Loads final dataset to BI    │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────┐
│   Preset BI      │
│ Dashboards       │
└──────────────────┘
```

---

## 🔄 Pipeline Components

### 1. Airflow – ETL Layer
- Extracts daily and hourly crypto data from CoinGecko.
- Loads data into Snowflake RAW schema:
  - RAW.COIN_GECKO_MARKET_DAILY
  - RAW.COIN_GECKO_OHLC
  - RAW.COIN_GECKO_MARKET_HOURLY

---

### 2. Snowflake – Data Warehouse
- RAW: Raw ingested API data
- ANALYTICS: Transformed indicators, forecasts, alerts
- SNAPSHOT: Historical SCD tracking

---

### 3. dbt – ELT Layer

Staging Models:
- stg_btc_market.sql
- stg_btc_ohlc.sql

Fact Model:
- fct_btc_indicators.sql computes:
  - Moving Averages (7, 30)
  - RSI (14)
  - Volatility
  - Momentum & returns(7 days)

Snapshots:
- snap_btc_market.sql
- snap_btc_ohlc.sql

---

### 4. Machine Learning – Forecasting
- Prophet-based forecasting
- 14-day prediction horizon
- Confidence intervals
- Stored in ANALYTICS.CRYPTO_FORECAST_FINAL

---

### 5. Alerts Engine
- RSI thresholds
- Moving average crossovers
- Volatility spikes

Stored in ANALYTICS.CRYPTO_ALERTS

---

### 6. Visualization – Preset BI
- Price trends
- Moving averages
- RSI analysis
- OHLC behavior
- Forecast overlays
- Alert monitoring
- Hourly intraday tracking

---

## ✅ Data Quality & Validation
- dbt tests for non-null keys and duplicates
- Snapshots track historical changes
- Airflow retries handle API failures

---

## 📂 Project Structure

```text
(repository root)
├── dags/
│   ├── etl_coin_gecko_data_exploration.py
│   ├── coin_gecko_market_hourly_etl_v1.py
│   ├── btc_elt_dbt_v1.py
│   ├── crypto_price_forecast.py
│   └── crypto_alerts_v1.py
├── dbt/
│   └── btc_elt/
│       ├── models/
│       ├── snapshots/
│       ├── tests/
│       ├── schema.yml
│       └── dbt_project.yml
├── Dockerfile
├── docker-compose.yaml
└── README.md
```

---

## ▶️ How to Run

1. From this repository root, start services:
```bash
docker compose up -d
```
*(If your Docker install only provides the legacy CLI, use `docker-compose up -d` instead.)*

2. Access Airflow:
http://localhost:8081  
Username: airflow  
Password: airflow  

3. Configure Airflow Variables:
coin_list = bitcoin,ethereum,binancecoin,solana,cardano  
forecast_days = 14  

4. Snowflake Connection:
- Connection ID: snowflake_conn
- Database: USER_DB_PEACOCK
- Warehouse: PEACOCK_QUERY_WH

5. Trigger DAGs:
- Daily ETL
- Hourly ETL
- dbt ELT
- Forecast DAG
- Alerts DAG

 ### **6. Validate Output**
```sql
SELECT * FROM ANALYTICS.FCT_BTC_INDICATORS LIMIT 50;
SELECT * FROM ANALYTICS.CRYPTO_FORECAST_FINAL LIMIT 50;
SELECT * FROM ANALYTICS.CRYPTO_ALERTS LIMIT 50;
```

### **7. Build Visualizations in Preset**
Connect to Snowflake → Create datasets → Build charts → Assemble dashboard. 

---
## 📊 Key Features

- Automated multi-DAG Airflow workflow  
- dbt transformations, testing, snapshots  
- Multi-coin Prophet forecasting  
- Intelligent alert generation  
- Interactive, real-time dashboards  

---
## ⚠️ Assumptions & Limitations

- Forecasts assume short-term continuation of historical trends.
- External events are not modeled.
- Alerts are indicator-based.
- API rate limits may impact ingestion.

---

## 🚀 Future Enhancements

- Streaming ingestion (Kafka)
- Advanced models (ARIMA, LSTM)
- Dynamic alert thresholds
- Anomaly detection
- Role-based dashboard access

---

## 📝 Conclusion

This project demonstrates a modern, scalable crypto analytics platform capable of processing real-time data, generating technical insights, producing ML-driven forecasts, and triggering actionable alerts. Through the integration of Airflow, Snowflake, dbt, Prophet, and Preset BI, the pipeline delivers a complete end-to-end analytics solution for multi-coin market intelligence.

## 📝 Dashboard
1. Cryptocurrency Daily OHLC and Long-Term Price Trend Analysis

This dashboard compares crypto price patterns through two views: The left chart shows daily OHLC (Open-High-Low-Close) data with maximum opening (green) and closing (yellow) prices ranging 80k-140k throughout 2026. The right chart displays long-term trends for Bitcoin, Ethereum, Binancecoin, and Cardano, with Bitcoin (yellow) dominating at 90k-120k. Both reveal mid-year peaks around 120k followed by declines to 90k by year-end.

<img width="1255" height="383" alt="Cryptocurrency Daily OHLC and Long-Term Price Trend Analysis" src="https://github.com/user-attachments/assets/ef6fa852-d4f6-44e2-b4e5-370b8d63eadd" />

2. Cryptocurrency Price Forecast and Alert Monitoring Dashboard

This dashboard provides comprehensive cryptocurrency trading analysis with three panels:
Left Panel - 14-Day Price Forecast
Middle Panel - Alerts Table: Displays trading alerts based on RSI, moving averages, and volume indicators. Current alerts include an RSI_OVERBOUGHT warning for Binancecoin and multiple BULLISH_TREND signals for Bitcoin, Binancecoin, Ethereum, and Cardano
Right Panel - Hourly Price Chart: Tracks real-time hourly price movements for five cryptocurrencies

<img width="1886" height="438" alt="Cryptocurrency Price Forecast and Alert Monitoring Dashboard" src="https://github.com/user-attachments/assets/e156c300-3484-49ac-b9e6-f7c14a88822c" />

3. Image of Price Comparision,MA7 and MA30,RSI Trend

This dashboard tracks cryptocurrency prices and market momentum. It shows:

Max Price: 125k (highest price reached across tracked coins)
RSI: 50.7 (neutral market momentum - not overbought or oversold)
Chart: Compares 5 cryptocurrencies (Bitcoin, Ethereum, Binancecoin, Solana, Ripple) over time, showing Bitcoin (yellow line) leading with prices fluctuating between 80k-125k, with a notable dip mid-period followed by partial recovery.

<img width="1280" height="418" alt="Image of Price Comparision,MA7 and MA30,RSI Trend" src="https://github.com/user-attachments/assets/36b5834c-5faa-47e9-abc7-4f00101023b2" />


