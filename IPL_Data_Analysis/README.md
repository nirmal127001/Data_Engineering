# IPL Data Analysis (apache-spark-project)

## Description
My purpose to build this project is to gain some handson experience on spark.
In this project, we harness the power of Apache Spark to perform comprehensive analysis on Indian Premier League (IPL) data. By leveraging Spark's robust capabilities in handling large datasets and performing parallel computations, we aim to uncover meaningful insights and patterns in IPL matches over multiple seasons.

## Dataset preview
This is the complete data of the IPL till 2017 (inclusive) and contains 5 following csv files.  
1. Ball_By_Ball.csv
2. Match.csv
3. Player.csv
4. Player_match.csv
5. Team.csv

Dataset Link: https://data.world/mkhuzaima/ipl-data-till-2017
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
