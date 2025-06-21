# NYC Taxi Data Engineering Pipeline

This project demonstrates a full data pipeline on Azure using:
- Azure Data Factory
- Azure Data Lake Gen2
- Azure Databricks
- Delta Lake
- Power BI

## Medallion Architecture
![Architecture](nyc-yellow-taxi/blob/main/Architecture.png)

## Steps:
1. Raw data ingested using NYC Taxi API → Data Factory → Bronze Layer.
2. Data transformation using Databricks → Silver Layer.
3. Delta Tables created → Gold Layer.
4. Connected Power BI to Gold layer for reporting.
