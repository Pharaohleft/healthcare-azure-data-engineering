# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Use legacy catalog to bypass permissions
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC -- 2. Create the View for the ULTIMATE table (Ignoring the missing 'department_kpis')
# MAGIC CREATE OR REPLACE VIEW default.gold_business_kpis AS 
# MAGIC SELECT * FROM delta.`abfss://gold@hospitaldataaustin.dfs.core.windows.net/hospital_business_suite`;
# MAGIC
# MAGIC -- 3. Verify it works immediately
# MAGIC SELECT * FROM default.gold_business_kpis LIMIT 5;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. READ SILVER STREAM ---
silver_df = spark.readStream \
    .format("delta") \
    .option("ignoreChanges", "true") \
    .load("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean")

# --- 2. THE 20-KPI LOGIC  ---
ultimate_kpis = silver_df.withColumn("hour", hour(col("admission_ts"))) \
    .groupBy("department").agg(
    
    # --- PILLAR 1: TRAFFIC & STAFFING ---
    count("patient_id").alias("1_total_admissions"),
    sum(when((col("hour") >= 6) & (col("hour") < 14), 1).otherwise(0)).alias("2_morning_shift_admits"),
    sum(when((col("hour") >= 14) & (col("hour") < 22), 1).otherwise(0)).alias("3_evening_shift_admits"),
    sum(when((col("hour") >= 22) | (col("hour") < 6), 1).otherwise(0)).alias("4_night_shift_admits"),
    round(sum("length_of_stay_hours"), 0).alias("5_total_bed_hours_consumed"),
    
    # --- PILLAR 2: SPEED & EFFICIENCY ---
    round(avg("length_of_stay_hours"), 1).alias("6_avg_los_hours"),
    round(min("length_of_stay_hours"), 1).alias("7_min_los_hours"),
    round(max("length_of_stay_hours"), 1).alias("8_max_los_hours"),
    round(stddev("length_of_stay_hours"), 1).alias("9_std_dev_los"),
    sum(when(col("length_of_stay_hours") < 4, 1).otherwise(0)).alias("10_rapid_turnover_count"),
    
    # --- PILLAR 3: BOTTLENECKS & RISK ---
    sum(when((col("length_of_stay_hours") >= 24) & (col("length_of_stay_hours") < 48), 1).otherwise(0)).alias("11_long_stay_1_2_days"),
    sum(when(col("length_of_stay_hours") >= 48, 1).otherwise(0)).alias("12_critical_stay_over_2_days"),
    

    round(count("patient_id").cast("double") / approx_count_distinct("bed_id").cast("double"), 1).alias("13_bed_turnaround_rate"),
    
    # Complexity Score
    round((avg("age") * 0.4) + (avg("length_of_stay_hours") * 0.6), 1).alias("14_complexity_score"),
    
    # --- PILLAR 4: DEMOGRAPHICS ---
    sum(when(col("age") < 18, 1).otherwise(0)).alias("16_pediatrics_count"),
    sum(when((col("age") >= 18) & (col("age") < 65), 1).otherwise(0)).alias("17_adults_count"),
    sum(when((col("age") >= 65) & (col("age") < 80), 1).otherwise(0)).alias("18_seniors_count"),
    sum(when(col("age") >= 80, 1).otherwise(0)).alias("19_critical_seniors_80_plus"),
    round(sum(when(col("gender") == "Female", 1).otherwise(0)).cast("double") / count("patient_id").cast("double"), 2).alias("20_female_ratio")
    
).withColumn(
    # --- SMART ALERTS ---
    "Recommendation_Status",
    when(col("12_critical_stay_over_2_days") > 5, " CRITICAL: Bed Blockage Detected")
    .when(col("9_std_dev_los") > 10, " WARNING: Process unstable")
    .when(col("19_critical_seniors_80_plus") > 10, "ℹ NOTICE: High Geriatric Load")
    .otherwise(" OPERATIONAL: Optimal")
)

print("  Analytics Engine Started...")

# --- 3. WRITE TO GOLD ---
query = ultimate_kpis.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "abfss://gold@hospitaldataaustin.dfs.core.windows.net/checkpoints/ultimate_kpis_v4") \
    .start("abfss://gold@hospitaldataaustin.dfs.core.windows.net/hospital_business_suite")

print(" Enterprise Gold Stream Live.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     Recommendation_Status,
# MAGIC     `1_total_admissions` as Admits,
# MAGIC     `6_avg_los_hours` as Avg_Speed,
# MAGIC     `12_critical_stay_over_2_days` as STUCK_PATIENTS,
# MAGIC     `14_complexity_score` as Difficulty_Level
# MAGIC FROM delta.`abfss://gold@hospitaldataaustin.dfs.core.windows.net/hospital_business_suite`
# MAGIC ORDER BY `14_complexity_score` DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     `2_morning_shift_admits` as Morning_Load,
# MAGIC     `3_evening_shift_admits` as Evening_Load,
# MAGIC     `4_night_shift_admits` as Night_Load,
# MAGIC     `19_critical_seniors_80_plus` as High_Care_Patients
# MAGIC FROM delta.`abfss://gold@hospitaldataaustin.dfs.core.windows.net/hospital_business_suite`
# MAGIC ORDER BY `4_night_shift_admits` DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     department, 
# MAGIC     Recommendation_Status as STATUS, 
# MAGIC     `1_total_admissions` as Total_Patients, 
# MAGIC     `6_avg_los_hours` as Avg_Wait_Time, 
# MAGIC     `14_complexity_score` as Difficulty_Score
# MAGIC FROM delta.`abfss://gold@hospitaldataaustin.dfs.core.windows.net/hospital_business_suite`
# MAGIC ORDER BY `14_complexity_score` DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     `2_morning_shift_admits` as Morning_Rush,
# MAGIC     `3_evening_shift_admits` as Evening_Rush,
# MAGIC     `4_night_shift_admits` as Night_Crew_Load,
# MAGIC     `19_critical_seniors_80_plus` as High_Risk_Seniors
# MAGIC FROM delta.`abfss://gold@hospitaldataaustin.dfs.core.windows.net/hospital_business_suite`
# MAGIC ORDER BY `4_night_shift_admits` DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     `13_bed_turnaround_rate` as Patients_Per_Bed,
# MAGIC     `10_rapid_turnover_count` as Easy_Cases,
# MAGIC     `11_long_stay_1_2_days` as Slow_Cases,
# MAGIC     `12_critical_stay_over_2_days` as STUCK_CASES
# MAGIC FROM delta.`abfss://gold@hospitaldataaustin.dfs.core.windows.net/hospital_business_suite`
# MAGIC ORDER BY `12_critical_stay_over_2_days` DESC