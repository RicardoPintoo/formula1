# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Keys using SAS token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo cluster
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

abc = dbutils.secrets.get(scope='f1Scope', key='SASToken')
print(abc)

# COMMAND ----------

sas_token = dbutils.secrets.get
print(sas_token)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.form1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.form1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.form1dl.dfs.core.windows.net", dbutils.secrets.get(scope='f1Scope', key='SASToken'))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@form1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@form1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


