# Tokyo-Olympic (Project on Azure Cloud)

## Description
Building Data Pipelines in Azure with Azure Synapse Analytics.  
My purpose in building this project is to gain hands-on experience in executing a data engineering project using Azure cloud services.
In this Microsoft Azure Data Engineering Project, I learned learn how to build a data pipeline using Azure Synapse Analytics, Azure Storage and Azure Synapse SQL pool to perform data analysis on the 2021 Olympics dataset.

## Architecture
![Alt text](Arcitecture.png)


## Dataset preview  
For this project, I will be working with the 2021 Olympics dataset. This includes the information on more than 11,000 athletes competing in 47 sports for 743 Teams in the Tokyo Olympics in 2021. This dataset includes information on the participating Teams, Athletes, Coaches, and Entries by gender. It includes their names, nationalities, sports they compete in, and name of coaches.  
The dataset contains 5 files as follows:    
1. Athletes.xlsx (Details about Athletes)      
2. Coaches.xlsx (Details about coaches, countries and disciplines along with event)  
3. EntriesGender.xlsx (Details about the Discipline and the number of females and males participating)  
4. Medals.xlsx (Contains the Medals and Scoreboard of countries that participated in Olympics)    
5. Teams.xlsx (Details about the Teams, discipline, Name of Country and the event)  

Dataset Link: https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo  

## Approach    
➡Downloaded dataset from source and uploaded extracted files into my gitub repository in csv format.    
➡Create an Azure Storage Account and created folders to store our data into **Azure Data Lake Storage(ADLS)**.   
➡Ingested raw data files from gitub(HTTP Server) to ADLS using **Azure Data Factory**.  
➡Created application in order to connect ADLS to Databricks using its credentials.  
➡Created **Azure Key Vault** to store the sensitive credentials of the application created.  
➡Created **Databricks** workspace and mounted ADLS to access raw data using app's credentials stored in key vault.    
➡Done few transormations and wrote the transformed files into separate folder back in ADLS.   
➡Created an **Azure Synapse Analytics** workspace and created database to store our transformed files as separate tables.    
➡Done some analysis using serverless SQL pool.

## Analysis     
|Query|Description|
|-----|-----------|
|Q1|Calculate the total number of medals won by each country.|
|Q2|Count the number of total athletes from each country.|
|Q3|Calculaate average number of entries by gender for each discipline.|
|Q4|Find the top countries with the highest number of gold medals.|

## Learnings
✅ Extract Data from APIs  
✅ Azure Services - DataBricks, DataFactory, Azure Data Lake Storage, Key Vault and Synapse Analytics  
✅ Writing Spark Code  
✅ SQL queries for analysis

