# Databricks notebook source
# Read the data raw wrote to the Bronze Delta table
bronze_df = spark.read.format("delta").load("abfss://bronze@hospitaldataaustin.dfs.core.windows.net/patient_data_raw")

display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType


full_secret = dbutils.secrets.get(scope="hospital-secrets", key="eventhub-conn").strip()

# Unpack the secret keys containers
eh_namespace = full_secret.split("Endpoint=sb://")[1].split(".")[0]
eh_sas_key_value = full_secret.split("SharedAccessKey=")[1]
eh_sas_key_name = full_secret.split("SharedAccessKeyName=")[1].split(";")[0]

# --- 2. CONFIGURATION (Updated with your Names) ---

eh_name = "patient-admission-stream" 
eh_bootstrap_servers = f"{eh_namespace}.servicebus.windows.net:9093"
eh_connection_string = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;SharedAccessKeyName={eh_sas_key_name};SharedAccessKey={eh_sas_key_value}"

print(f" Connecting to Namespace: {eh_namespace}")
print(f" Target Topic (Event Hub): {eh_name}")

kafka_options = {
    "kafka.bootstrap.servers": eh_bootstrap_servers,
    "subscribe": eh_name,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    # Using 'kafkashaded' to bypass the LoginModule error:3 hr betweeen azure event hubs and kafka
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{eh_connection_string}";',
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

print(" Attempting to connect...")

# --- 3. EXECUTION ---
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Parse & Write
df_parsed = df.select(
    col("value").cast("string").alias("payload"),
    current_timestamp().alias("ingested_at")
)

# i Changed checkpoint to '_v3' to start fresh
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://bronze@hospitaldataaustin.dfs.core.windows.net/checkpoints/bronze_raw_kafka_v3") \
    .start("abfss://bronze@hospitaldataaustin.dfs.core.windows.net/patient_data_raw")

print(" Stream is live! Go start your Python Data Generator.")

# COMMAND ----------

fs.azure.account.key.hospitaldataaustin.dfs.core.windows.net {{secrets/hospital-secrets/storage-key}}
spark.jars.packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22

# COMMAND ----------

#  what the connection string looks like
secret = dbutils.secrets.get(scope="hospital-secrets", key="eventhub-conn")

print(f"First 10 chars: {secret[:10]}")
print(f"Total length: {len(secret)}")

# Simulating the logic to see the resulting address
try:
    endpoint = secret.split(";")[0].replace("Endpoint=sb://", "").strip("/")
    print(f"Attempting to connect to: {endpoint}:9093")
except:
    print(" Could not parse endpoint. Secret format is likely wrong.")

# COMMAND ----------

try:
    print(spark.conf.get("spark.jars.packages"))
except:
    print(" No libraries configured in Spark Config!")