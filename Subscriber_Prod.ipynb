# Databricks notebook source
# MAGIC %run "/Workspace/optum1/prod/connectors_prod"

# COMMAND ----------

# MAGIC %run "/Workspace/optum1/prod/Generic_Prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

subsc_df= read_bronze_csv("subscriber")


# COMMAND ----------

#transformations
subsc_df = subsc_df.fillna({'first_name':'Visitor/NA' , "Elig_ind":'N'})

subsc_df = subsc_df.drop("Phone")
subsc_df = subsc_df.withColumn("subscriber_age",(months_between(current_date(),col("Birth_date"))/12).cast("integer"))

subsc_df = subsc_df.drop("Birth_date")


# COMMAND ----------

subsc_df = subsc_df.drop("patient_birth_date")

# COMMAND ----------

subsc_df = subsc_df.withColumn("Subgrp_id",when((col("Subgrp_id").isNull()) & (col("sub_id") == "SUBID10022"),"S110")\
                                          .when((col("Subgrp_id").isNull()) & (col("sub_id") == "SUBID10049"),"S107").otherwise("Subgrp_id"))

# COMMAND ----------

write2silver(subsc_df,"subscriber_S.csv")