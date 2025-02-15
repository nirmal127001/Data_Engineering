# Data Engineering Projects
This repository contains multiple data engineering projects built mainly on Azure. Each project focuses on different aspects of data processing, validation, and analytics.  
Each Directory contains a fully implemented & well documented project with a detailed explanation.  
These projects demonstrate real-world data engineering skills, covering data ingestion, transformation, validation, and storage using industry-standard tools.

## Projects
* **AP Morgan Project**: Focuses on processing CSV files using Azure Data Lake Storage, Databricks, and Azure SQL Server. It includes data validation, handling duplicates, and storing validated data in Delta Tables.  
* **Connected Vehicle Project**: Processes JSON files containing vehicle tracking data using Azure Data Lake Storage, Databricks, and Azure SQL Server. Data is ingested into the Landing folder, validated using schema rules stored in Azure SQL Server, and then processed in Databricks. Valid data is moved to the Staging folder, while rejected files are sent to the Rejected folder. Secure authentication is managed via Azure Key Vault, ensuring data quality and efficient processing before storing in Delta Tables.
* **IPL Data Analysis**: Utilizes Spark to analyze IPL cricket data, extracting insights and performing data transformations.  
* **Tokyo-Olympic Data Engineering Project on Azure**: Implements data engineering workflows on Azure, processing and analyzing Tokyo Olympic data using services like Azure Data Factory and Azure Synapse.  
* **YouTube Analysis Project**: Analyzes YouTube data to extract trends and insights, employing data pipelines and visualization techniques.  
