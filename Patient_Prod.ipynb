# Databricks notebook source
# MAGIC %run "/Workspace/optum1/connectors/"

# COMMAND ----------

# MAGIC %run "/Workspace/optum1/Generic/"

# COMMAND ----------

adls_connect()

# COMMAND ----------

Pat_df= read_bronze_csv("Patient_records")


# COMMAND ----------

#transformations
Pat_df = Pat_df.fillna({'Patient_name':'Visitor/NA'})
Pat_df = Pat_df.withColumn("patient_phone",concat(substring(col("patient_phone"),1,6),lit("*****"),substring(col("patient_phone"),-2,2)))
Pat_df = Pat_df.withColumn("patient_age",(months_between(current_date(),col("patient_birth_date"))/12).cast("integer"))
Pat_df = Pat_df.drop("patient_birth_date")

# COMMAND ----------

Pat_df = Pat_df.drop("patient_birth_date")

# COMMAND ----------

write2silver(Pat_df,"Patient_S.csv")