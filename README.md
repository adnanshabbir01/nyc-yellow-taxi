# NYC Taxi Data Engineering Pipeline

This project demonstrates a full data pipeline on Azure using:
- Azure Data Factory
- Azure Data Lake Gen2
- Azure Databricks
- Delta Lake
- Power BI

## 📥 Data Source

This project uses data from the [NYC Taxi & Limousine Commission Open Data API](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

The specific dataset used is:  
[Yellow Taxi Trip Records – 2024]

## Medallion Architecture
![Architecture](https://github.com/adnanshabbir01/nyc-yellow-taxi/blob/main/Architecture.png)

## Steps:
1. Raw data ingested using NYC Taxi API → Data Factory → Bronze Layer.
2. Data transformation using Databricks → Silver Layer.
3. Delta Tables created → Gold Layer.
4. Connected Power BI to Gold layer for reporting.
