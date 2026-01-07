# Databricks notebook source
#  print the first 3 characters of your key to verify it's no typeo
# without exposing the whole passowrds or keys on the notebook
key = dbutils.secrets.get(scope="hospital-secrets", key="storage-key")
print(f"Secret starts with: {key[:3]}")
print(f"Total length of secret: {len(key)}")

# COMMAND ----------

# 1. Test Storage Connection
try:
    storage_test = dbutils.fs.ls("abfss://bronze@hospitaldataaustin.dfs.core.windows.net/")
    print(" Storage Account Connected!")
except Exception as e:
    print(f" Storage Failed: {e}")

# 2. Test Key Vault Access
try:
    #  check if Databricks can 'see' the secrets troubleshooting
    secrets_list = dbutils.secrets.list("hospital-secrets")
    print(" Key Vault Scope Linked!")
except Exception as e:
    print(f" Key Vault Failed: {e}")

# COMMAND ----------

display(dbutils.secrets.list("hospital-secrets"))