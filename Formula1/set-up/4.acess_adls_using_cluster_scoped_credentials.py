# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Lake using cluster scoped credentials
# MAGIC 1. Set the spark config  fs.azure.account.key in the cluster config file
# MAGIC 2. List files from demo cluster
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@form1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@form1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


