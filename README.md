# nyc-yellow-taxi
End-to-end data pipeline using Azure services and NYC Taxi data. Data is ingested via ADF, stored in Data Lake Gen2 (Bronze), transformed with Databricks (Silver), and written as Delta tables (Gold). Power BI connects to the Gold layer for reporting. Secure access is managed via Microsoft Entra ID.
