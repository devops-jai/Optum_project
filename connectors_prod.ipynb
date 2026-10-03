# Databricks notebook source
def adls_connect():
    spark.conf.set("fs.azure.account.key.optum1adlsstg.dfs.core.windows.net",
    dbutils.secrets.get(scope="optum1dbx", key="adlskey"))
    return "ADLS Connection Established"

# COMMAND ----------

def list_bronze_files():
    display(dbutils.fs.ls("abfss://optum1@optum1adlsstg.dfs.core.windows.net/Bronze/"))
    return "Bronze files Listed"


# COMMAND ----------

def list_silver_files():
    display(dbutils.fs.ls("abfss://optum1@optum1adlsstg.dfs.core.windows.net/Silver/"))
    return "Silver files Listed"

# COMMAND ----------

def list_gold_files():
    display(dbutils.fs.ls("abfss://optum1@optum1adlsstg.dfs.core.windows.net/Gold/"))
    return "gold files Listed"

# COMMAND ----------

def read_silver_csv(file_name):
    data = spark.read.csv("abfss://optum1@optum1adlsstg.dfs.core.windows.net/Silver/"+file_name+".csv", header=True, inferSchema=True)
    return data

# COMMAND ----------

def read_gold_csv(file_name):
    data = spark.read.csv("abfss://optum1@optum1adlsstg.dfs.core.windows.net/Gold/"+file_name+".csv", header=True, inferSchema=True)
    return data

# COMMAND ----------

def read_bronze_csv(file_name):
    data = spark.read.csv("abfss://optum1@optum1adlsstg.dfs.core.windows.net/Bronze/"+file_name+".csv", header=True, inferSchema=True)
    return data

# COMMAND ----------

def read_bronze_json(file_name):
    data = spark.read.json("abfss://optum1@optum1adlsstg.dfs.core.windows.net/Bronze/"+file_name+".json")
    return data

# COMMAND ----------

def write2database(df, table_name):
    hostname = dbutils.secrets.get(scope="optum1dbx", key="azuresqlserver")
    port = dbutils.secrets.get(scope="optum1dbx", key="azuresqlserverport")
    DatabaseName = dbutils.secrets.get(scope="optum1dbx", key="dbname")
    DBProperties = {
    "user": dbutils.secrets.get(scope="optum1dbx", key="dbuser"),
    "password":dbutils.secrets.get(scope="optum1dbx", key="dbpass")}
    urloftarget = "jdbc:sqlserver://{0}:{1};database={2}".format(hostname,port,DatabaseName)
    output = df.write.jdbc(url=urloftarget, table=table_name, mode="overwrite", properties=DBProperties)
    print("Sucessfully written in Azure SQL Database************")

    

# COMMAND ----------

def write2silver(df,file_name):
    silver_path = "abfss://optum1@optum1adlsstg.dfs.core.windows.net/Silver/"
    temp_path = f"{silver_path}/output_temp"
    final_path = f"{silver_path}/{file_name}"
    df.write.mode("overwrite").option("header","true").csv(temp_path)
    files = dbutils.fs.ls(temp_path)
    csv_file = [file.path for file in files if file.path.endswith(".csv")][0]
    print(csv_file)
    dbutils.fs.mv(csv_file,final_path)



# COMMAND ----------

def write2gold(df,file_name):
    gold_path = "abfss://optum1@optum1adlsstg.dfs.core.windows.net/Gold/"
    temp_path = f"{gold_path}/output_temp"
    print(temp_path)
    final_path = f"{gold_path}/{file_name}"
    print(final_path)
    df.write.mode("overwrite").option("header","true").csv(temp_path)
    files = dbutils.fs.ls(temp_path)
    csv_file = [file.path for file in files if file.path.endswith(".csv")][0]
    print(csv_file)
    dbutils.fs.mv(csv_file,final_path)
    dbutils.fs.rm(temp_path,recurse = True)  
    print("*****Sucessfully written in gold Layer")  

# COMMAND ----------

