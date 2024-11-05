# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret / password for the Application
# MAGIC 3. Set Spark Config with App/Client Id, Directory/ Tenant Id & Secret 
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = butils.secrets.get(scope="f1Scope",key="clientId")
tenant_id = dbutils.secrets.get(scope="f1Scope",key="tenantId")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="f1Scope",key="clientSecret")

spark.conf.set("fs.azure.account.auth.type.form1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.form1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.form1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.form1dl.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.form1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@form1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@form1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


