# Databricks notebook source
# Reading the data directly from data.world url as a workaround because spark dataframe is unable to read data directly 
# from url and hence pandas is used to convert csv to pandas df.
import pandas as pd
df = pd.read_csv('https://query.data.world/s/irxczuog4sdx6zhyxfu62i7lmrdoxj')
df.head()
#df.columns

# COMMAND ----------



# COMMAND ----------

#converting pandas dataframe df to spark dataframe sparkDF
df=spark.createDataFrame(df) 
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;
# MAGIC --%fs ls /FileStore/tables
# MAGIC --describe formatted agg_covid_19_india_status;
# MAGIC --drop table agg_covid_19_india_status;
# MAGIC --drop table latest_covid_19_india_status_csv;

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/

# COMMAND ----------

# to create spark DF from csv file already downloaded but since this method is replaced with
# pandas df and then converted it to spark df, it has been commented now
# File location and type
#file_location = "/FileStore/tables/India_COVID_19_Status.csv"

#file_type = "csv"

# CSV options
#infer_schema = "true"
#first_row_is_header = "true"
#delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
#df = spark.read.format(file_type) \
  #.option("inferSchema", infer_schema) \
  #.option("header", first_row_is_header) \
  #.option("sep", delimiter) \
  #.load(file_location)

#print(type(df))

#display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

#Create the final df to be saved into the covid19_india_aggregate_data table.
# new columns are derived which indicates the covid status of new cases/deaths/recoveries reported on previous day 
# along with aggregate data.
from pyspark.sql.functions import round

df = df.select(df["State Name"].alias("State_Name"), 
               df["Positive Cases (Today)"].alias("Total_Cases"), 
               df["Active Cases (Today)"].alias("Total_Active_Cases"),
               df["Cured Cases (Today)"].alias("Total_Recoveries"), 
               df["Death Cases (Today)"].alias("Total_Deaths"),
               (df["Positive Cases (Today)"] - df["Positive Cases (Yesterday)"]).alias("New_Cases"),
               (df["Active Cases (Today)"] - df["Active Cases (Yesterday)"]).alias("New_Active_Cases"),
               (df["Cured Cases (Today)"] - df["Cured Cases (Yesterday)"]).alias("New_Recoveries"), 
               (df["Death Cases (Today)"] - df["Death Cases (Yesterday)"]).alias("New_Deaths"), 
               (round((df["Active Cases (Today)"]/df["Positive Cases (Today)"])*100)).alias("Active_ratio"),
               df["Last Updated (IST)"].alias("Last_Updated_IST") )

# COMMAND ----------

# This will only be used for inserting the data first time when table does not exist
# We will be appending the data rather than overwritining so that previous days aggregate is 
# also present for any furture analysis. using last_updated column we can identify data belongs to which date.
# df.write.mode("append").saveAsTable("covid19_india_aggregate")

# COMMAND ----------

#create a dataframe of existing data in covid19_india_aggregate
#df_temp = spark.sql("select * from covid19_india_aggregate")
#df_temp.count()

# COMMAND ----------

# create new df after union of new data df and existing data in table covid19_india_aggregate
df_final = df.union(spark.sql("select * from covid19_india_aggregate")).drop_duplicates()
df_final.count()

# COMMAND ----------

display(df_final.orderBy("State_Name"))

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("covid19_india_aggregate")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from covid19_india_aggregate;

# COMMAND ----------

#%fs rm -r /user/hive/warehouse/covid19_india_aggregate_data/

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;
# MAGIC describe formatted covid19_india_aggregate;

# COMMAND ----------

print(spark.catalog.listTables())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from covid19_india_aggregate

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --# Pie chart showing top 10 states in terms Total Cases State-wise
# MAGIC select state_name as `States`, Total_Cases, Total_Active_Cases, Total_Recoveries, Total_Deaths, New_Cases, New_Active_Cases, New_Deaths
# MAGIC from covid19_india_aggregate order by Total_Cases desc limit 10;

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --Pie chart showing top 10 states in terms Total Deaths State-wise
# MAGIC select State_name as `States`, Total_Cases, Total_Active_Cases, Total_Recoveries, Total_Deaths, New_Cases, New_Active_Cases, New_Deaths
# MAGIC from covid19_india_aggregate order by Total_Deaths desc limit 10;

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --# Bar chart showing top 10 states in terms pf Total Cases vs Total Recoveries
# MAGIC select state_name as `States`, Total_Cases, Total_Active_Cases, Total_Recoveries, Total_Deaths, New_Cases, New_Active_Cases, New_Deaths
# MAGIC from covid19_india_aggregate order by Total_Cases desc limit 10;

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --# Pie chart showing top 10 states in terms of Total Active Cases State-wise
# MAGIC select state_name as `States`, Total_Cases, Total_Active_Cases, Total_Recoveries, Total_Deaths, New_Cases, New_Active_Cases, New_Deaths
# MAGIC from covid19_india_aggregate order by Total_Active_Cases desc limit 10;

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --# Bar chart showing top 10 states in terms of New Active cases vs New Active Cases
# MAGIC select state_name as `States`, Total_Cases, Total_Active_Cases, Total_Recoveries, Total_Deaths, New_Cases, New_Active_Cases, New_Deaths
# MAGIC from covid19_india_aggregate order by New_Cases desc limit 10;
