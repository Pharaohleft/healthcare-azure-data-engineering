# Databricks notebook source


print(" Cleaning up corrupted Silver tables...")

# 1. Delete the Data Table (The one expecting Integers) error solving becuase int assumption before
dbutils.fs.rm("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean", True)

# 2. Delete the Checkpoints (The memory of the old schema) schema fault, with char
dbutils.fs.rm("abfss://silver@hospitaldataaustin.dfs.core.windows.net/checkpoints", True)

print(" Silver Layer Wiped Clean. You can now re-run the streaming cell.")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. DEFINE SCHEMA ---
json_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("admission_time", StringType(), True),
    StructField("discharge_time", StringType(), True),
    StructField("bed_id", IntegerType(), True),
    StructField("hospital_id", IntegerType(), True)
])

print("Correct Schema Defined.")

# --- 2. READ STREAM ---
bronze_df = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .load("abfss://bronze@hospitaldataaustin.dfs.core.windows.net/patient_data_raw")

# --- 3. PARSE & CLEAN (Fixed Indentation) ---

silver_df = (bronze_df
    .withColumn("parsed_data", from_json(col("payload"), json_schema))
    .select("parsed_data.*", "ingested_at")
    
    # Convert string times to real Timestamps
    .withColumn("admission_ts", to_timestamp(col("admission_time")))
    .withColumn("discharge_ts", to_timestamp(col("discharge_time")))
    
    # Feature: Calculate Length of Stay (in Hours)
    .withColumn("length_of_stay_hours", 
                (col("discharge_ts").cast("long") - col("admission_ts").cast("long")) / 3600)
    
    .drop("admission_time", "discharge_time")
    .filter(col("patient_id").isNotNull())
)

print(" Transformation Pipeline Ready...")

# --- 4. WRITE STREAM TO SILVER ---
#  set to '_v3' to ensure a fresh start
query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://silver@hospitaldataaustin.dfs.core.windows.net/checkpoints/patient_silver_admissions_v3") \
    .start("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean")

print(" Silver Stream Started! Processing Admission Data.")

# COMMAND ----------

display(spark.read.load("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean"))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. DEFINE THE SCHEMA ---


json_schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("event_time", StringType(), True),     #  cast this to Timestamp later
    StructField("department", StringType(), True),     
    StructField("heart_rate", IntegerType(), True),
    StructField("bp_systolic", IntegerType(), True),
    StructField("bp_diastolic", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("admission_type", StringType(), True)  
])

print(" Schema Defined.")

# COMMAND ----------

display(spark.read.load("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean"))

# COMMAND ----------

# Read the Bronze table to see the raw JSON
raw_df = spark.read.format("delta").load("abfss://bronze@hospitaldataaustin.dfs.core.windows.net/patient_data_raw")

# Show me the raw text
display(raw_df.select("payload").limit(5))

# COMMAND ----------

# --- 2. READ STREAM FROM BRONZE ---
#  read from the Delta table, NOT Kafka. it decouples the layers.
bronze_df = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .load("abfss://bronze@hospitaldataaustin.dfs.core.windows.net/patient_data_raw")

# --- 3. PARSE & CLEAN (SILVER LOGIC) ---
silver_df = bronze_df \
    .withColumn("parsed_data", from_json(col("payload"), json_schema)) \
    .select("parsed_data.*", "ingested_at") \
    .withColumn("event_timestamp", to_timestamp(col("event_time"))) \
    .drop("event_time") \
    .filter(col("patient_id").isNotNull())  # Remove bad records

# Feature Engineering: Add "Risk Level" based on Vitals 
silver_enhanced_df = silver_df.withColumn(
    "risk_level",
    when((col("heart_rate") > 120) | (col("temperature") > 39.0), "High")
    .when((col("heart_rate") > 100) | (col("temperature") > 38.0), "Medium")
    .otherwise("Low")
)

print(" Transformation Pipeline Ready...")

# --- 4. WRITE STREAM TO SILVER ---
# write to a new container called 'silver'
query = silver_enhanced_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://silver@hospitaldataaustin.dfs.core.windows.net/checkpoints/patient_silver_v1") \
    .start("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean")

print(" Silver Stream Started! Cleaning data in real-time.")