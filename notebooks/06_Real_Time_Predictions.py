# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC -- Create the view for Power BI
# MAGIC CREATE OR REPLACE VIEW default.gold_patient_predictions AS 
# MAGIC SELECT * FROM delta.`abfss://gold@hospitaldataaustin.dfs.core.windows.net/patient_predictions`;
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT * FROM default.gold_patient_predictions ORDER BY admission_ts DESC LIMIT 5;

# COMMAND ----------

from pyspark.ml import PipelineModel
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. LOAD THE SAVED BRAIN (MODEL) ---
print(" Loading the AI Model...")
model_path = "abfss://gold@hospitaldataaustin.dfs.core.windows.net/models/los_predictor_v1"
model = PipelineModel.load(model_path)

# --- 2. READ THE LIVE STREAM ---
silver_stream = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .load("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean")

# --- 3. APPLY AI TO LIVE DATA ---
# The model adds a column called 'prediction' (Predicted Hours)
predictions_df = model.transform(silver_stream)

# --- 4. ADDAssumed demanded BUSINESS LOGIC (RECOMMENDATIONS) ---
# take the raw prediction and turn it into human advice
final_predictions = predictions_df.withColumn(
    "predicted_los_hours", round(col("prediction"), 1)
).withColumn(
    "risk_alert",
    when(col("predicted_los_hours") > 96, " CRITICAL: Likely Long Term Stay")
    .when(col("predicted_los_hours") > 48, " HIGH: Monitor Closely")
    .otherwise(" LOW: Routine Discharge")
).select(
    "patient_id", "department", "age", "gender", 
    "admission_ts", "predicted_los_hours", "risk_alert"
)

print(" AI Prediction Engine Started...")

# --- 5. WRITE PREDICTIONS TO GOLD ---
query = final_predictions.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://gold@hospitaldataaustin.dfs.core.windows.net/checkpoints/predictions_v1") \
    .start("abfss://gold@hospitaldataaustin.dfs.core.windows.net/patient_predictions")

print(" Live Predictions are flowing to Gold.")