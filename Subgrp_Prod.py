# Databricks notebook source
# MAGIC %run "/Workspace/optum1/prod/connectors_prod"

# COMMAND ----------

# MAGIC %run "/Workspace/optum1/prod/Generic_Prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

Subgrp_df= read_bronze_csv("subgroup")


# COMMAND ----------

Subgrp_df = Subgrp_df.withColumn("subgrp_id",split(col('subgrp_id'),','))


# COMMAND ----------

Subgrp_df= Subgrp_df.withColumn("subgrp_id",explode(col("subgrp_id")))

# COMMAND ----------

write2silver(Subgrp_df,"subgroup_S.csv")