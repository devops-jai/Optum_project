# Databricks notebook source
# MAGIC %run "/Workspace/optum1/prod/connectors_prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

Hos_df = read_silver_csv("Hospital_S")

# COMMAND ----------

Pat_df = read_silver_csv("Patient_S")

Hos_df = read_silver_csv("Hospital_S")

Claims_df = read_silver_csv("Claims_S")

Dis_df = read_silver_csv("Disease_S")

Grp_df = read_silver_csv("Group_S")

Subgrp_df = read_silver_csv("subgroup_S")

subsc_df = read_silver_csv("subscriber_S")



# COMMAND ----------

from pyspark.sql.functions import col, explode, split, trim


# Patient
Pat_df = (
    Pat_df
    .withColumnRenamed("city", "pat_city")
    .withColumnRenamed("hospital_id", "patient_hospital_id")
    .withColumnRenamed("disease_name", "patient_disease_name")
)


# Hospital
Hos_df = (
    Hos_df
    .withColumnRenamed("Hospital_id", "hospital_id")
    .withColumnRenamed("city", "hospital_city")
    .withColumnRenamed("country", "hospital_country")
)


# Claims
Claims_df = (
    Claims_df
    .withColumnRenamed("SUB_ID", "claim_sub_id")
    .withColumnRenamed("disease_name", "claim_disease_name")
)


# Subscriber
subsc_df = (
    subsc_df
    .withColumnRenamed("City", "subscriber_city")
    .withColumnRenamed("Country", "subscriber_country")
    .withColumnRenamed("Gender", "subscriber_gender")
    .withColumnRenamed("Zip Code", "subscriber_zip_code")
    .withColumnRenamed("Subgrp_id", "subscriber_subgrp_id")
)


# Subgroup
Subgrp_df = (
    Subgrp_df
    .withColumn(
        "group_id",
        explode(
            split(
                trim(col("subgrp_id")),
                ","
            )
        )
    )
    .withColumn(
        "group_id",
        trim(col("group_id"))
    )
    .drop("subgrp_id")
)


# Group
Grp_df = (
    Grp_df
    .withColumnRenamed("grp_id", "grp_group_id")
    .withColumnRenamed("country", "group_country")
    .withColumnRenamed("zip_code", "group_zip_code")
)


# Disease
Dis_df = (
    Dis_df
    .withColumnRenamed("disease_name", "dis_disease_name")
    .withColumnRenamed("subgrp_id", "dis_subgrp_id")
)


# ============================================================
# JOIN
# ============================================================

final_df = (
    Claims_df
    .join(Pat_df, "patient_id", "left")
    .join(
        Hos_df,
        Pat_df.patient_hospital_id == Hos_df.hospital_id,
        "left"
    )
    .join(
        subsc_df,
        Claims_df.claim_sub_id == subsc_df.sub_id,
        "left"
    )
    .join(
        Subgrp_df,
        subsc_df.subscriber_subgrp_id == Subgrp_df.subgrp_sk,
        "left"
    )
    .join(
        Grp_df,
        Subgrp_df.group_id == Grp_df.grp_group_id,
        "left"
    )
    .join(
        Dis_df,
        Subgrp_df.subgrp_sk == Dis_df.dis_subgrp_id,
        "left"
    )
)


# ============================================================
# NULL VALUES
# String  -> N/A
# Number  -> 0
# ============================================================

string_cols = []
numeric_cols = []

for field in final_df.schema.fields:

    dtype = field.dataType.simpleString()

    if dtype == "string":
        string_cols.append(field.name)

    elif dtype in ["int", "bigint", "double", "float", "long", "short", "decimal"]:
        numeric_cols.append(field.name)


if string_cols:
    final_df = final_df.fillna("N/A", subset=string_cols)

if numeric_cols:
    final_df = final_df.fillna(0, subset=numeric_cols)




# COMMAND ----------

write2gold(final_df,"Optum1_Gold")

# COMMAND ----------

write2database(final_df,"Optum1_Tb")