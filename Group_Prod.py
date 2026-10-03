# Databricks notebook source
# MAGIC %run "/Workspace/optum1/prod/connectors_prod"

# COMMAND ----------

# MAGIC %run "/Workspace/optum1/prod/Generic_Prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

Grp_df= read_bronze_csv("group")


# COMMAND ----------

write2silver(Grp_df,"Group_S.csv")
