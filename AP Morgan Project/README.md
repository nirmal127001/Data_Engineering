# AP Morgan (Project on Azure Cloud)
## Description



## Architecture
![Alt text](Architecture.png)

Dataset preview
For this project, I will be working with the 2021 Olympics dataset. This includes the information on more than 11,000 athletes competing in 47 sports for 743 Teams in the Tokyo Olympics in 2021. This dataset includes information on the participating Teams, Athletes, Coaches, and Entries by gender. It includes their names, nationalities, sports they compete in, and name of coaches.
The dataset contains 5 files as follows:

Athletes.xlsx (Details about Athletes)
Coaches.xlsx (Details about coaches, countries and disciplines along with event)
EntriesGender.xlsx (Details about the Discipline and the number of females and males participating)
Medals.xlsx (Contains the Medals and Scoreboard of countries that participated in Olympics)
Teams.xlsx (Details about the Teams, discipline, Name of Country and the event)
Dataset Link: https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo

Approach
➡Internal Application sends CSV file in Azure Data Lake Storage.  
➡Validation needed to apply on this follows:  
  ✔Check for duplicate rows. If it contains duplicate rows, file needs to be rejected.  
  ✔Need to validate the date format for all the date fields. Date column names and desired date format is stored in an Azure SQL Server. If validation fails, file     will be rejected.  
➡Move all the rejected files to the Reject folder.  
➡Move all the passed files to the Staging folder.  
➡Write the passed files as a Delta table in Azure Databricks.  

## Learnings
✅ Extract Data from APIs  
✅ Azure Services - DataBricks, DataFactory, Azure Data Lake Storage, Key Vault and Synapse Analytics  
✅ Writing Spark Code  
✅ SQL queries for analysis  
