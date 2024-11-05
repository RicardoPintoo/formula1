# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Keys using access keys
# MAGIC 1. Set the spark config  fs.azure.account.key
# MAGIC 2. List files from demo cluster
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.form1dl.dfs.core.windows.net",
    ""
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@form1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@form1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


