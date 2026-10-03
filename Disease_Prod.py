# Databricks notebook source
# MAGIC %run "/Workspace/optum1/prod/connectors_prod"

# COMMAND ----------

# MAGIC %run "/Workspace/optum1/prod/Generic_Prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

Dis_df= read_bronze_csv("disease")


# COMMAND ----------

write2silver(Dis_df,"Disease_S.csv")