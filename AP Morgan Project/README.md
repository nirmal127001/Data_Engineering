# AP Morgan (Project on Azure Cloud)
## Description  
This project processes CSV files using Azure Data Lake Storage, Databricks, and Azure SQL Server. Files are ingested into the Landing folder, then validated in Databricks using schema rules stored in Azure SQL Server. Valid files are moved to the Staging folder, while rejected files (due to duplicates or incorrect formats) are sent to the Rejected folder. Secure authentication is managed via Azure Key Vault. This ensures data quality and efficient processing before storing in Delta Tables. 🚀


## Architecture
![Alt text](Architecture.png)

## Data Table preview  
The Product Table contains details about various products, including pricing, unique identifiers, and important timestamps.  

Table Structure:
| Column Name   | Data Type  | Description |
|--------------|-----------|-------------|
| **ProductId**  | String | Unique identifier for the product. |
| **Price**      | Integer | Price of the product. |
| **guid**       | UUID | Globally unique identifier for the product. |
| **StartDate**  | Date | Date when the product becomes active. |
| **EndDate**    | Date | Date when the product is no longer available. |
| **CreateDate** | Date | Date when the product was first created. |
| **ModifiedDate** | Date | Date when the product details were last updated. |

## Approach  
- Internal Application sends CSV file in Azure Data Lake Storage.  
- Validation needed to apply on this follows:  
   - Check for duplicate rows. If it contains duplicate rows, file needs to be rejected.  
   - Need to validate the date format for all the date fields. Date column names and desired date format is stored in an Azure SQL Server. If validation fails, file        will be rejected.  
- Move all the rejected files to the Reject folder.  
- Move all the passed files to the Staging folder.  
- Write the passed files as a Delta table in Azure Databricks.  

## Azure Services Used:  
✅ Azure Data Factory  
✅ Data Factory Pipeline  
✅ Azure Key Vault  
✅ Azure SQL DB  
✅ SSMS  
✅ AWS S3 Bucket  
✅ Connect ADF to Databricks  
✅ Connect Databricks to SQL Server  
✅ Connect Databricks to ADLS  
✅ Connect S3 to Azure Cloud  
✅ Triggers  
✅ SAS token  
✅ Create Secrets scope in Databricks  
✅ Store secretes in Key Vault and access them  
