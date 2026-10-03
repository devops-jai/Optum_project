# Databricks notebook source
# MAGIC %run "/Workspace/optum1/prod/connectors_prod"

# COMMAND ----------

# MAGIC %run "/Workspace/optum1/prod/Generic_Prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

Hos_df= read_bronze_csv("Hospital")


# COMMAND ----------

display(Hos_df.limit(10))

# COMMAND ----------

Hos_df = Hos_df.replace("NaN",None)  #correction

# COMMAND ----------

Hos_df = Hos_df.fillna({'State': 'UT'}) # transformation

# COMMAND ----------

write2silver(Hos_df,"Hospital_S.csv")