-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL Implementation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Setting Directory to show code reusability

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # To show code reusability, you can enter any of the year (2023, 2021, 2020) of the clinical trial dataset
-- MAGIC
-- MAGIC year = 2023
-- MAGIC file = 'clinicaltrial_' + str(year)
-- MAGIC pharma = 'pharma'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Loading any clinical trial dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Reading the clinical trial file from the path
-- MAGIC clinical_trial = sc.textFile("/FileStore/tables/" + file + ".csv")
-- MAGIC header = clinical_trial.first()
-- MAGIC clinical_trial = clinical_trial.filter(lambda y: y != header)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Replacing the dataset containing irregular expression with an empty string
-- MAGIC # And also spliting with the delimiters
-- MAGIC clinical_trial_RDD = clinical_trial.map(lambda x: x.replace('"', '') \
-- MAGIC                                                     .replace(',', '') \
-- MAGIC                                                     .split('\t') if len(x.split('\t')) == 14 else x.replace('"', '').split('|'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define a function to fix the length of each row in the RDD to match a specified length
-- MAGIC def fix_row_length(row, length):
-- MAGIC     return row + [None] * (length - len(row))
-- MAGIC     
-- MAGIC def process_rdd_based_on_length(rdd):
-- MAGIC     length = len(rdd.first())
-- MAGIC     if length == 14:
-- MAGIC         return rdd.map(lambda row: fix_row_length(row, 14))
-- MAGIC     elif length == 9:
-- MAGIC         return rdd.map(lambda row: fix_row_length(row, 9))
-- MAGIC     else:
-- MAGIC         raise ValueError("Unexpected row length: {}".format(length))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinical_trial_RDD = process_rdd_based_on_length(clinical_trial_RDD)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType
-- MAGIC
-- MAGIC # Defining the schemas for the dataset
-- MAGIC schema_1 = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Study_Title", StringType(), True),
-- MAGIC     StructField("Acronym", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Collaborators", StringType(), True),
-- MAGIC     StructField("Enrollment", StringType(), True),
-- MAGIC     StructField("Funder_Type", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Study_Design", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC schema_2 = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Submission", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True)
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Geting the length of the first row in the RDD
-- MAGIC row_len = len(clinical_trial_RDD.first())
-- MAGIC
-- MAGIC # Defining the schema based on the row length
-- MAGIC selected_schema = schema_1 if row_len == 14 else schema_2
-- MAGIC
-- MAGIC # Creating a dataframe from the selected schema and the RDD
-- MAGIC clinical_trial_df = spark.createDataFrame(clinical_trial_RDD, selected_schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Creating a temp view for our SQL tasks
-- MAGIC clinical_trial_df.createOrReplaceTempView("clinical_trial_SQL")

-- COMMAND ----------

SELECT * FROM clinical_trial_SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Loading the "pharma.csv" dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Reading the pharma.csv file from the directory using the spark_session
-- MAGIC pharma_df = spark\
-- MAGIC         .read.options(delimiter = ",")\
-- MAGIC         .csv("/FileStore/tables/" + pharma + ".csv", header = True, inferSchema = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Creating a temp view for our SQL tasks
-- MAGIC pharma_df.createOrReplaceTempView("pharma_SQL")

-- COMMAND ----------

SELECT * FROM pharma_SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Confirming that the temp view for the dataset has been created

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### QUESTION AND ANSWER

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Importing the neccessary library for visualization
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import pandas as pd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 1
-- MAGIC The number of studies in the dataset. You must ensure that you explicitly check distinct studies.

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) AS Number_of_studies
FROM clinical_trial_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 2
-- MAGIC You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.

-- COMMAND ----------

SELECT Type, COUNT(*) AS count
FROM clinical_trial_sql
WHERE Type IS NOT NULL AND Type != ''
GROUP BY Type
ORDER BY count DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _sqldf = spark.sql("SELECT Type, COUNT(*) AS count FROM clinical_trial_sql WHERE Type IS NOT NULL AND Type != '' GROUP BY Type ORDER BY count DESC")
-- MAGIC
-- MAGIC type_counts_df = _sqldf.toPandas()
-- MAGIC
-- MAGIC # Plotting the bar chart
-- MAGIC plt.figure(figsize=(7, 7))
-- MAGIC plt.bar(type_counts_df["Type"], type_counts_df["count"], color='red')
-- MAGIC plt.xlabel('Type')
-- MAGIC plt.ylabel('Frequency')
-- MAGIC plt.title(f'Frequency of Different Types of Studies in {year}')
-- MAGIC plt.xticks(rotation=45, ha='right')
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 3
-- MAGIC The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

SELECT Conditions, COUNT(*) AS count
FROM clinical_trial_sql
WHERE Conditions IS NOT NULL
GROUP BY Conditions
ORDER BY count DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _sqldf = spark.sql("SELECT Conditions, COUNT(*) AS count FROM clinical_trial_sql WHERE Conditions IS NOT NULL GROUP BY Conditions ORDER BY count DESC LIMIT 5")
-- MAGIC top_5_cond_df = _sqldf.toPandas()
-- MAGIC
-- MAGIC # Plotting the pie chart
-- MAGIC plt.figure(figsize=(7, 7))
-- MAGIC plt.pie(top_5_cond_df["count"], labels=top_5_cond_df["Conditions"], autopct='%1.1f%%', startangle=140)
-- MAGIC plt.axis('equal')
-- MAGIC plt.title(f'Top 5 Conditions in Clinical Trials in {year}')
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 4
-- MAGIC Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the Parent Company column contains all possible pharmaceutical companies.

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS count
FROM clinical_trial_sql
LEFT JOIN pharma_SQL ON clinical_trial_sql.Sponsor = pharma_SQL.Company
WHERE pharma_SQL.Company IS NULL
GROUP BY Sponsor
ORDER BY count DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Extracting names and counts
-- MAGIC _sqldf = spark.sql("SELECT Sponsor, COUNT(*) AS count FROM clinical_trial_sql LEFT JOIN pharma_SQL ON clinical_trial_sql.Sponsor = pharma_SQL.Company WHERE pharma_SQL.Company IS NULL GROUP BY Sponsor ORDER BY count DESC LIMIT 10")
-- MAGIC spons_counts_df = _sqldf.toPandas()
-- MAGIC
-- MAGIC # Plotting the bar chart
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.barh(spons_counts_df["Sponsor"], spons_counts_df["count"], color='red')
-- MAGIC plt.xlabel('Count')
-- MAGIC plt.ylabel('Sponsor')
-- MAGIC plt.title(f'Top 10 Sponsors (Non-Pharmaceutical) in {year}')
-- MAGIC plt.gca().invert_yaxis()
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 5
-- MAGIC Plot number of completed studies for each month in 2023. You need to include your visualization as well as a table of all the values you have plotted for each month.

-- COMMAND ----------

-- Execute SQL query and store result in a temporary view
CREATE OR REPLACE TEMP VIEW completed_trials_2023 AS
SELECT 
    CASE 
        WHEN MONTH(Completion) = 1 THEN 'January'
        WHEN MONTH(Completion) = 2 THEN 'February'
        WHEN MONTH(Completion) = 3 THEN 'March'
        WHEN MONTH(Completion) = 4 THEN 'April'
        WHEN MONTH(Completion) = 5 THEN 'May'
        WHEN MONTH(Completion) = 6 THEN 'June'
        WHEN MONTH(Completion) = 7 THEN 'July'
        WHEN MONTH(Completion) = 8 THEN 'August'
        WHEN MONTH(Completion) = 9 THEN 'September'
        WHEN MONTH(Completion) = 10 THEN 'October'
        WHEN MONTH(Completion) = 11 THEN 'November'
        ELSE 'December'
    END AS Month,
    COUNT(*) AS Count
FROM clinical_trial_sql
WHERE Status = 'COMPLETED' 
    AND Completion IS NOT NULL 
    AND YEAR(Completion) = 2023
GROUP BY MONTH(Completion)
ORDER BY MONTH(Completion);

-- COMMAND ----------

-- Displaying the table
SELECT * FROM completed_trials_2023

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Query the temporary view
-- MAGIC _sqldf = spark.sql("SELECT * FROM completed_trials_2023")
-- MAGIC monthly_counts_df = _sqldf.toPandas()
-- MAGIC
-- MAGIC # Plotting the bar chart
-- MAGIC plt.figure(figsize=(11, 8))
-- MAGIC plt.bar(monthly_counts_df["Month"], monthly_counts_df["Count"], color='red')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title(f'Number of Completed Studies for Each Month in {year}')
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### FURTHER ANALYSIS
-- MAGIC Top 10 companies with failed/terminated clinical trials

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS Terminated_Trials
FROM clinical_trial_sql
WHERE UPPER(Status) = 'TERMINATED'
GROUP BY Sponsor
ORDER BY Terminated_Trials DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Extracting names and counts
-- MAGIC _sqldf = spark.sql("SELECT Sponsor, COUNT(*) AS Terminated_Trials FROM clinical_trial_sql WHERE UPPER(Status) = 'TERMINATED' GROUP BY Sponsor ORDER BY Terminated_Trials DESC LIMIT 10")
-- MAGIC
-- MAGIC # Convert Spark DataFrame to Pandas DataFrame
-- MAGIC term_df = _sqldf.toPandas()
-- MAGIC
-- MAGIC # Plotting the bar chart
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(term_df['Sponsor'], term_df['Terminated_Trials'], color='red')
-- MAGIC plt.xlabel('Company')
-- MAGIC plt.ylabel('Terminated Trials')
-- MAGIC plt.title(f'Top Companies with Terminated Clinical Trials in {year}')
-- MAGIC plt.xticks(rotation=45, ha='right')
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
