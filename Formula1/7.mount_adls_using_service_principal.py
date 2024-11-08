# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id and client secret from key vault
# MAGIC 2. Set spark Config with App/Client id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utility mount the storage 
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@form1dl.dfs.core.windows.net/",
  mount_point = "/mnt/form1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/form1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/form1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


