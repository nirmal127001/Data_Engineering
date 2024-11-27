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
##Approach:  
➡Downloaded dataset from source and uploaded extracted files into my gitub repository in csv format.  
➡Create an azure storage account and created folders to store our data into ADLS. 
➡Ingested raw data files from gitub(HTTP Server) to ADLS using Azure Data Factory. 
➡Create an azure synapse analytics workspace.  
➡Create a SQL pool in azure synapse workspace.  
➡Create table structure in SQL pool.  
➡Create a data pipeline to ingest data from azure storage into SQL pool tables.  

### Queries performed to get answers for the following questions:     
|Query|Description|
|-----|-----------|
|Q1|Top scoring batsman per season.|
|Q2|Impact of Winning Toss on Match Outcome.|
|Q3|Scores by Venue.|
|Q4|Team Performance After Winning Toss.|
|Q5|Number of Wins Per Team Per Season.|

## Learnings
✔ Python & PySpark  
✔ SQL  
✔ Apache Spark Basics and Databricks  
✔ Writing transformation logic  
✔ Visuallizing data for insights

