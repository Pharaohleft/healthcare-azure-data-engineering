# Databricks notebook source
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. LOAD TRAINING DATA ---
#  use the Silver data (Cleaned history) to teach the model
print(" Loading historical data...")
df = spark.read.format("delta").load("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean")

# --- 2. PREPARE FEATURES (Convert Text to Numbers) ---
# .  convert "Male" -> 0, "Female" -> 1, .
categorical_cols = ["gender", "department"]
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in categorical_cols]

# Combine Age + Gender + Department into one "Features" list
assembler = VectorAssembler(
    inputCols=["age", "gender_vec", "department_vec"],
    outputCol="features"
)

# --- 3. TRAIN THE MODEL (Random Forest) ---
rf = RandomForestRegressor(featuresCol="features", labelCol="length_of_stay_hours", numTrees=20)

#  all together
pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

print(" Training the Brain... (This learns patterns from your data)")
model = pipeline.fit(df)
print(" Model Trained!")

# --- 4. SAVE THE MODEL ---
#  save this "Brain" to storage so we can use it in the live stream later
model.write().overwrite().save("abfss://gold@hospitaldataaustin.dfs.core.windows.net/models/los_predictor_v1")
print(" Model Saved to Storage.")

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. LOAD HISTORICAL DATA (TRAINING SET) ---
#  use the clean Silver data to train the brain
df = spark.read.format("delta").load("abfss://silver@hospitaldataaustin.dfs.core.windows.net/patient_data_clean")

# --- 2. PREPARE FEATURES ---
#   convert "Gender" and "Department" into numbers.

# Define categorical columns
categorical_cols = ["gender", "department"]
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in categorical_cols]

# Assemble all inputs into a single "features" vector
# Inputs: Age, Gender(vec), Dept(vec)
assembler = VectorAssembler(
    inputCols=["age", "gender_vec", "department_vec"],
    outputCol="features"
)

# --- 3. DEFINE THE MODEL (RANDOM FOREST) ---
rf = RandomForestRegressor(featuresCol="features", labelCol="length_of_stay_hours", numTrees=20)

# --- 4. BUILD & TRAIN PIPELINE ---
# A Pipeline chains all the steps (Index -> Encode -> Assemble -> Train) together
pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

print("Training Model... This may take a minute...")
model = pipeline.fit(df)
print(" Model Trained Successfully!")

# --- 5. SAVE THE MODEL ---
#  save it so the streaming job can load it instantly
model.write().overwrite().save("abfss://gold@hospitaldataaustin.dfs.core.windows.net/models/los_predictor_v1")
print(" Model Saved to Storage.")