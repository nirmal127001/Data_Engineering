## End-To-End Databricks Project 

This project demonstrates how to implement a Medallion Architecture (Bronze-Silver-Gold) pattern in Databricks using Delta Lake.    
It involves ingesting data in CSV, JSON, and Parquet formats, storing them in DBFS, and transforming them through structured layers into Delta Tables.  

### Bronze Layer
- Raw files (CSV, JSON, Parquet) are uploaded to DBFS.
- Ingested into Bronze Delta Tables with minimal transformation.

### Silver Layer
- Cleaned, filtered, and structured data from Bronze layer.
- Null checks, schema standardization, and deduplication performed.

### Gold Layer
- Aggregated or business-ready datasets for reporting or analytics.


