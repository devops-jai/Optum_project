# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run "/Workspace/optum1/prod/connectors_prod"

# COMMAND ----------

# MAGIC %run "/Workspace/optum1/prod/Generic_Prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

Claims_df= read_bronze_json("Claims")


# COMMAND ----------

claims.count()
claims.groupby("Claim_or_Rejected").count().show()
claims.select("Claim_or_Rejected").distinct().show(6)

# COMMAND ----------

Claims_df = Claims_df.replace("NaN",None)  #correction

# COMMAND ----------

# DBTITLE 1,Transformation
#Transformation
Claims_df = Claims_df.fillna({"Claim_or_Rejected": "N"})
Claims_df = Claims_df.withColumn("claim_date", to_date(timestamp_millis(get_json_object(col("claim_date"), "$['date']").cast("long"))))


# COMMAND ----------

write2silver(Claims_df,"Claims_S.csv")