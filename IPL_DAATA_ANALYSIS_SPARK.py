# Databricks notebook source
#not needed in databricks
from pyspark.sql import SparkSession
#create session
spark = SparkSession.builder.appName("IPL_Data_Analysis").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Access s3 bucket data directly without mounting

# COMMAND ----------

#generated keys in aws to access s3 bucket files and downloaded csv file for the same. Uploaded the csvn file containing keys in DBFS(uploaded from local machine) for security purpose
keys = spark.read.csv("dbfs:/FileStore/nirmal_dev_accessKeys.csv",inferSchema = True, header = True)
keys.printSchema()
access_key = keys.select("Access key ID").collect()[0][0]
secret_key = keys.select("Secret access key").collect()[0][0]

# COMMAND ----------

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# COMMAND ----------

match_df = spark.read.csv("s3://ipldatabucket2017/Match.csv",inferSchema = True, header = True)
ball_by_ball_df = spark.read.csv("s3://ipldatabucket2017/Ball_By_Ball.csv",inferSchema = True, header = True)
player_match_df = spark.read.csv("s3://ipldatabucket2017/Player_match.csv",inferSchema = True, header = True)
team_df = spark.read.csv("s3://ipldatabucket2017/Team.csv",inferSchema = True, header = True)
player_df = spark.read.csv("s3://ipldatabucket2017/Player.csv",inferSchema = True, header = True)

# COMMAND ----------

match_df.display()
# ball_by_ball_df.display()
# player_match_df.display()
# team_df.display()
# player_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Access s3 bucket data by mounting s3 bucket in databricks

# COMMAND ----------

encoded_secret_key = secret_key.replace("/","%2F")
bucket_name = "ipldatabucket2017"
mount_name = "s3_mount"
dbutils.fs.unmount(f"/mnt/{mount_name}") # to unmount
dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{bucket_name}",f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

file_location = "dbfs:/mnt/s3_mount/Team.csv"
df = spark.read.csv(file_location,inferSchema = True, header = True)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC We might have mismatch of data types as we can't rely on spark's inferschema so we will create our own schema

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,BooleanType,DateType,DecimalType

ball_by_ball_schema = StructType(
    [
        StructField("match_id", IntegerType(), True),
        StructField("over_id", IntegerType(), True), 
        StructField("ball_id", IntegerType(), True), 
        StructField("innings_no", IntegerType(), True), 
        StructField("team_batting", IntegerType(), True), 
        StructField("team_bowling", IntegerType(), True), 
        StructField("striker_batting_position", IntegerType(), True), 
        StructField("extra_type", StringType(), True), 
        StructField("runs_scored", IntegerType(), True), 
        StructField("extra_runs", IntegerType(), True), 
        StructField("wides", IntegerType(), True), 
        StructField("legbyes", IntegerType(), True), 
        StructField("byes", IntegerType(), True), 
        StructField("noballs", IntegerType(), True), 
        StructField("penalty", IntegerType(), True), 
        StructField("bowler_extras", IntegerType(), True), 
        StructField("out_type", StringType(), True), 
        StructField("caught", BooleanType(), True), 
        StructField("bowled", BooleanType(), True), 
        StructField("run_out", BooleanType(), True), 
        StructField("lbw", BooleanType(), True), 
        StructField("retired_hurt", BooleanType(), True), 
        StructField("stumped", BooleanType(), True), 
        StructField("caught_and_bowled", BooleanType(), True), 
        StructField("hit_wicket", BooleanType(), True), 
        StructField("obstructingfeild", BooleanType(), True), 
        StructField("bowler_wicket", BooleanType(), True), 
        StructField("match_date", DateType(), True), 
        StructField("season", IntegerType(), True), 
        StructField("striker", IntegerType(), True), 
        StructField("non_striker", IntegerType(), True), 
        StructField("bowler", IntegerType(), True), 
        StructField("player_out", IntegerType(), True), 
        StructField("fielders", IntegerType(), True), 
        StructField("striker_match_sk", IntegerType(), True), 
        StructField("strikersk", IntegerType(), True), 
        StructField("nonstriker_match_sk", IntegerType(), True), 
        StructField("nonstriker_sk", IntegerType(), True), 
        StructField("fielder_match_sk", IntegerType(), True), 
        StructField("fielder_sk", IntegerType(), True), 
        StructField("bowler_match_sk", IntegerType(), True), 
        StructField("bowler_sk", IntegerType(), True), 
        StructField("playerout_match_sk", IntegerType(), True), 
        StructField("battingteam_sk", IntegerType(), True), 
        StructField("bowlingteam_sk", IntegerType(), True), 
        StructField("keeper_catch", BooleanType(), True), 
        StructField("player_out_sk", IntegerType(), True), 
        StructField("matchdatesk", DateType(), True)
    ]
)

# COMMAND ----------

ball_by_ball_df = spark.read.format('csv')\
                            .option("header","true")\
                            .schema(ball_by_ball_schema)\
                            .option("dateFormat","M/d/y")\
                            .load("s3://ipldatabucket2017/Ball_By_Ball.csv")

ball_by_ball_df.display()

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True), 
    StructField("match_id", IntegerType(), True), 
    StructField("team1", StringType(), True), 
    StructField("team2", StringType(), True), 
    StructField("match_date", DateType(), True), 
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True), 
    StructField("city_name", StringType(), True), 
    StructField("country_name", StringType(), True), 
    StructField("toss_winner", StringType(), True), 
    StructField("match_winner", StringType(), True), 
    StructField("toss_name", StringType(), True), 
    StructField("win_type", StringType(), True), 
    StructField("outcome_type", StringType(), True), 
    StructField("manofmach", StringType(), True), 
    StructField("win_margin", IntegerType(), True), 
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

match_df = spark.read.format('csv')\
                            .option("header","true")\
                            .schema(match_schema)\
                            .option("dateFormat","M/d/y")\
                            .load("s3://ipldatabucket2017/Match.csv")

match_df.display()

# COMMAND ----------

match_df.select("match_winner").distinct().show()
match_df.select("toss_winner").distinct().show()

# COMMAND ----------

#to handle null values in match df
match_df = match_df.fillna({'match_winner': 'Not Applied'})
match_df = match_df.fillna({'toss_winner': 'Not Applied'})

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True), 
    StructField("playermatch_key", StringType(), True), 
    StructField("match_id", DecimalType(), True), 
    StructField("player_id", IntegerType(), True), 
    StructField("player_name", StringType(), True), 
    StructField("dob", DateType(), True), 
    StructField("batting_hand", StringType(), True), 
    StructField("bowling_skill", StringType(), True), 
    StructField("country_name", StringType(), True), 
    StructField("role_desc", StringType(), True), 
    StructField("player_team", StringType(), True), 
    StructField("opposit_team", StringType(), True), 
    StructField("season_year", IntegerType(), True), # Use StringType() for year 
    StructField("is_manofthematch", BooleanType(), True), 
    StructField("age_as_on_match", IntegerType(), True), 
    StructField("isplayers_team_won", BooleanType(), True), 
    StructField("batting_status", StringType(), True), 
    StructField("bowling_status", StringType(), True), 
    StructField("player_captain", StringType(), True), 
    StructField("opposit_captain", StringType(), True), 
    StructField("player_keeper", StringType(), True), 
    StructField("opposit_keeper", StringType(), True)
])

# COMMAND ----------

player_match_df = spark.read.format('csv').option("header", "true")\
                                          .schema(player_match_schema)\
                                          .option("dateFormat",'M/d/y')\
                                          .load("s3://ipldatabucket2017/Player_match.csv")
player_match_df.display()

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

# COMMAND ----------

team_df = spark.read.format('csv')\
                    .option("header","true")\
                    .schema(team_schema)\
                    .load('s3://ipldatabucket2017/Team.csv')

team_df.display()

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(),True),
    StructField("player_id", IntegerType(),True),
    StructField("player_name", StringType(),True),
    StructField("dob", DateType(),True),
    StructField("batting_hand", StringType(),True),
    StructField("bowling_skill", StringType(),True),
    StructField("country_name", StringType(),True),
])

# COMMAND ----------

player_df = spark.read.format('csv')\
                      .option('header',"true")\
                      .schema(player_schema)\
                      .option("dateFormat",'M/d/y')\
                      .load('s3://ipldatabucket2017/Player.csv')

player_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Spark Data Transformations to make it suitable for the analysis

# COMMAND ----------

from pyspark.sql.functions import *

#Filter to include only valid deliveries(excludng wide and no balls)
ball_by_ball_df = ball_by_ball_df.filter((col("wides") == 0) & (col("noballs") == 0))

#Aggregation : Calculate the total and average runs scored in each match and inning
total = ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
)

# COMMAND ----------

#Window Function : Calculate running total of runs in each match for each over
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("match_id","innings_no").orderBy(col("over_id").desc())
ball_by_ball_df=ball_by_ball_df.withColumn("RunningRuns",sum("runs_scored").over(windowSpec))


# COMMAND ----------

 #Conditional Column : Flag for high impact balls (Either a wicket or more than 6 runs including extras)
 ball_by_ball_df = ball_by_ball_df.withColumn(
     "high_impact",
     when(
         (col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True), True
     ).otherwise(False)
 )

# COMMAND ----------

ball_by_ball_df.display()
#all transformations will happen when it's executed (Action) called lazy evaluation

# COMMAND ----------

# Function to make a string into Short form (eg 'Royal Challengers Bangalore' into 'RCB'
def inShort(str):
    str = str.split(" ")
    lst = []
    for i in str:
        if(i not in ['tied','abandoned','NULL','null']):
            lst.append(i[0])
        else:
            lst.append(i)
    newStr =  ''.join(lst)
    return newStr
# inShort('nulls')
inShort_udf = udf(inShort, StringType())

# COMMAND ----------

from pyspark.sql.functions import year,month,dayofmonth,when

#extracting year,month, and day from the match date 
match_df = match_df.withColumn("year", year("match_date"))
match_df = match_df.withColumn("month", month("match_date"))
match_df = match_df.withColumn("day", dayofmonth("match_date"))

#High margin win:  categorizing win margins into high, medium, and low.
match_df = match_df.withColumn(
    "win_margin_category",
    when(col("win_margin") >= 100, 'High')
    .when((col("win_margin") >= 50) & (col("win_margin") < 100),'Medium')
    .otherwise('Low')
    )

#Analyse the impact of the toss : who wins the toss and the match
match_df = match_df.withColumn(
    "toss_match_winner",
    when(col('toss_winner') == col('match_winner'), "Yes").otherwise("No")
) 
#Adding column for Short Name of each team so that we can use efficiently in plotting graph
match_df = match_df.withColumn(
    "team1_in_short",
    inShort_udf(match_df["team1"])
) 
match_df = match_df.withColumn(
    "team2_in_short",
    inShort_udf(match_df["team2"])
) 
match_df = match_df.withColumn(
    "toss_winner_in_short",
    inShort_udf(match_df["toss_winner"])
) 
match_df = match_df.withColumn(
    "match_winner_in_short",
    inShort_udf(match_df["match_winner"])
) 


match_df.display()


# COMMAND ----------

match_df.select('match_winner_in_short').distinct().show()
# match_df.select('match_winner').distinct().show()

# COMMAND ----------

player_df.select("batting_hand").distinct().show()

# COMMAND ----------

from pyspark.sql.functions import lower,regexp_replace

#Normalise and clean batting_hand column values
player_df = player_df.withColumn(
    "batting_hand0",
    lower(regexp_replace("batting_hand", "[^a-zA-Z0-9]", "")) 
    #Here, it’s replacing any characters in batting hand col that are not letters (a-z, A-Z) or numbers (0-9) with an empty string, effectively removing them.
)
player_df = player_df.withColumn(
    "batting_hand_new",
    when(
        col("batting_hand0").contains("left"), "Left-handed"
    ).otherwise("Right-handed") #will only conatain two distinct values
)
player_df = player_df.drop("batting_hand0")

player_df.display()

# COMMAND ----------

from pyspark.sql.functions import when,col,current_date

#Add veteran status column based on player's age
player_match_df = player_match_df.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >=35, "Veteran").otherwise("Non-Vetran")
)

#Dynamic column to calculate year since debut
player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

player_match_df.display()

# COMMAND ----------

#Converting all df into sql table so that we can query
ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

top_scoring_batsman_per_season = spark.sql("""
       SELECT p.player_name,m.season_year,SUM(b.runs_scored) as total_runs
       FROM ball_by_ball b
       JOIN match m ON b.match_id = m.match_id  
       JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id
       JOIN player p ON p.player_id = pm.player_id 
       GROUP BY p.player_name,m.season_year
       ORDER BY m.season_year,total_runs DESC                                  
                                           """)
top_scoring_batsman_per_season.show()

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
    SELECT m.match_id, m.toss_winner_in_short, m.toss_name, m.match_winner_in_short,
        CASE WHEN m.toss_winner_in_short = m.match_winner_in_short THEN 'Won' ELSE 'Lost' END AS match_outcome
    FROM match m
    WHERE m.toss_name IS NOT NULL
    ORDER BY m.match_id
                                           """)
toss_impact_individual_matches.show()

                            

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
toss_impact_pd = toss_impact_individual_matches.toPandas()

#Creating a countplot to show win/loss after winning toss
plt.figure(figsize=(15,6)) #defines area taken by the graph
sns.countplot(x='toss_winner_in_short', hue='match_outcome',data=toss_impact_pd)
plt.title("Impact of Winning Toss on Match Outcome")
plt.xlabel('Toss Winner_in_short')
plt.ylabel('Number of Matches')
plt.legend(title = 'Match Outcome')
# plt.xticks(rotation = 90) #to rotate x axis labels to 90 degree for better readability
plt.tight_layout()
plt.show

# COMMAND ----------

# MAGIC %md
# MAGIC From this graph we can conclude that whenever CSK won toss they likely loses the game and when KXP wins toss they likely win the match 
# MAGIC

# COMMAND ----------

scores_by_venue = spark.sql("""
     SELECT venue_name,AVG(total_runs) AS average_score, MAX(total_runs) AS highest_score
     FROM(
       SELECT ball_by_ball.match_id, match.venue_name,SUM(runs_scored) as total_runs
       FROM ball_by_ball
       JOIN match ON ball_by_ball.match_id = match.match_id
       GROUP BY ball_by_ball.match_id,match.venue_name
     )            
     GROUP BY venue_name
     ORDER BY highest_score ASC           
                            """)

scores_by_venue.display()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
scores_by_venue_pd = scores_by_venue.toPandas()

# Plot
plt.figure(figsize=(20,15)) #defines area taken by the graph
bars = sns.barplot(x='highest_score', y='venue_name',data=scores_by_venue_pd)

plt.title("Scores by Venue")
plt.xlabel('Average Score')
plt.ylabel('Venue')
# plt.xticks(rotation = 90) #to rotate x axis labels to 90 degree for better readability
plt.tight_layout()
plt.show

# COMMAND ----------

team_toss_win_performance = spark.sql("""
      SELECT team1_in_short,count(*) AS matches_played,
      SUM(CASE WHEN toss_winner_in_short = match_winner_in_short THEN 1 ELSE 0 END) AS win_after_toss
      FROM match
      WHERE toss_winner_in_short = team1_in_short        
      GROUP BY team1_in_short
      ORDER BY win_after_toss                        
                                      """)
team_toss_win_performance.show()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
team_toss_win_performance_pd = team_toss_win_performance.toPandas()

#Creating a countplot to show win/loss after winning toss
plt.figure(figsize=(12,6)) #defines area taken by the graph
sns.barplot(y='win_after_toss', x='team1_in_short',data=team_toss_win_performance_pd)
plt.title("Team Performance After Winning Toss")
plt.ylabel('Wins After Winning Toss')
plt.xlabel('Team')
# plt.xticks(rotation = 90) #to rotate x axis labels to 90 degree for better readability
plt.tight_layout()
plt.show


# COMMAND ----------

# MAGIC %md
# MAGIC CSK performed the best when they won the toss

# COMMAND ----------

# match_df.display()
match_df.select("team1").distinct().show()

# COMMAND ----------

season_wise_team_performance = spark.sql("""
      SELECT team1, season_year,
      SUM(CASE WHEN team1 = match_winner THEN 1 ELSE 0 END) AS wins
      FROM match 
      GROUP BY team1,season_year
      ORDER BY season_year DESC                            
                                         """)
season_wise_team_performance.show()

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

season_wise_team_performance_df = season_wise_team_performance.toPandas()

# Create bar plot
plt.figure(figsize=(20, 12))

# Define custom color palette 
custom_palette = custom_palette = { 
                                   'Sunrisers Hyderabad': '#FF4500', 
                                   'Chennai Super Kings': '#1E90FF', 
                                   'Deccan Chargers': '#0000FF', 
                                   'Kochi Tuskers Kerala': '#FF7F50', 
                                   'Rajasthan Royals': '#FFD700', 
                                   'Gujarat Lions': '#FF6347', 
                                   'Royal Challengers Bangalore': '#000000', 
                                   'Kolkata Knight Riders': '#800080', 
                                   'Rising Pune Supergiants': '#FF6347', 
                                   'Kings XI Punjab': '#FFD700', 
                                   'Pune Warriors': '#DC143C', 
                                   'Delhi Daredevils': '#FF4500', 
                                   'Mumbai Indians': '#FFD700' }

barplot = sns.barplot(x='season_year', y='wins', hue='team1', data=season_wise_team_performance_df, palette=custom_palette)

# Add numbers on bars
for p in barplot.patches:
    barplot.annotate(format(p.get_height(), '.1f'),
                     (p.get_x() + p.get_width() / 2., p.get_height()),
                     ha = 'center', va = 'center',
                     xytext = (0, 9),
                     textcoords = 'offset points')

# Add title and labels
plt.title('Number of Wins Per Team Per Season')
plt.xlabel('Season Year')
plt.ylabel('Number of Wins')
plt.legend(title='Teams', bbox_to_anchor=(1.01, 1), loc='upper left')

# Display the plot
plt.tight_layout()
plt.show()


# COMMAND ----------


