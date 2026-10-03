# Databricks notebook source
import pyspark
from pyspark.sql.functions import *

# COMMAND ----------

def rows_columns_count(df):
    return df.count(),len(df.columns)


# COMMAND ----------

def check_missing_values(df,lst_cl):
    missing_values = {}
    for i in lst_cl:
        a = df.filter(col(i).isNull()).count()
        missing_values[i]=a
    return missing_values


# COMMAND ----------

def display_data(df):
    return display(df.limit(6))

# COMMAND ----------

def check_duplicates(df,cl):
    a = df.select(cl).distinct().count()
    b = df.select (cl).count()
    if  a == b:
      print("No duplicates")
    else:
      c= b - a
      print("There are:",c,"duplicates")

# COMMAND ----------

def check_missing_values_percent_v1(df,lst_cl):
    global missing_values_percent_less_than_75
    global missing_values_percent_more_than_75
    missing_values_percent_less_than_75 = {}
    missing_values_percent_more_than_75 = {}
    b= df.count()
    for i in lst_cl:
        a = df.filter(col(i).isNull()).count()
        c= (a/b) * 100
        if c >= 75:
            missing_values_percent_more_than_75[i]=c
        else:
            missing_values_percent_less_than_75[i]=c
    return ({"missing_values_percent_more_than_75": missing_values_percent_more_than_75, "missing_values_percent_less_than_75": missing_values_percent_less_than_75})

# COMMAND ----------

def check_string_as_nan(df):
    result = {}
    for i in df.columns:
        result[i]=df.filter(col(i).like("NaN")).count()
    return result

# COMMAND ----------

def masking_phone(c1):
    masked_phone = c1[:3] + "*****"+c1[-2]
    return masked_phone

