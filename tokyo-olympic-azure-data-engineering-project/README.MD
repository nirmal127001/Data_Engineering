# Tokyo-Olympic (Project on Azure Cloud)

## Description
My purpose in building this project is to gain hands-on experience in executing a data engineering project using Azure cloud services.
In this project, we harness the power of Apache Spark to perform comprehensive analysis on Indian Premier League (IPL) data. By leveraging Spark's robust capabilities in handling large datasets and performing parallel computations, we aim to uncover meaningful insights and patterns in IPL matches over multiple seasons.

## Architecture
![Alt text](Arcitecture.png)


## Dataset preview
This is the complete data of the 2021-olympics-in-tokyo and contains 5 following excel files:    
1. Athletes.xlsx  
2. Coaches.xlsx  
3. EntriesGender.xlsx  
4. Medals.xlsx  
5. Teams.xlsx  

Dataset Link: https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo
## Steps:
➡ Downloaded dataset from the above link and uploaded to amazon s3 bucket to replicate real world scenario.  
➡ Used Databricks platform to efficiently write pySpark code without worrying about compute resources. Connected my s3 bucket to databricks notebook to use all 
   the dataset files.  
➡ Explored all the available tables, cleaned it, and transformed them according to our requirements to make them more sensible. This makes it easier to fetch 
   insights without worrying about any anomaly in the data.  
➡ Converted all the transformed datasets into sql table to query efficiently.  
➡ Performed queries and plotted graph to draw insights.

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

