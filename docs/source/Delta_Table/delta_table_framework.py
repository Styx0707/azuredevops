# Databricks notebook source
# MAGIC %run "../Framework/Configuration Management"

# COMMAND ----------

BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
HISTORIZED_RAW_CONTAINER=get_parameter("GLOBAL","HISTORIZED_RAW_CONTAINER")

print(BRONZE_CATALOG_NAME)
print(HISTORIZED_RAW_CONTAINER)

# COMMAND ----------

##### Initialization ############
source_path = "/mnt/standarized-raw-data/wyscout/"
entity = "areas"
sub_entity = ""
date= "*/*/*/"
#file_name = "competitions_matches_195_2023_04_25"
file_path = "{0}{1}{2}/*/*/*/{3}".format(source_path,entity,sub_entity,file_name)
display(file_path)

# COMMAND ----------

##### check if entity or sub entity is present or not ###########
if "" == sub_entity:
    table_name = entity
    file_name = entity+"*"
    print(table_name)
    #file_path = source_path + entity + "/" + date #+  file_name
    
    #display(file_path)
    target_path = "/mnt/historized-raw-data/wyscout/"+entity+"/"
    schema = "/mnt/historized-raw-data/wyscout/"+entity+"/schema/"
    checkpoint_path = "/mnt/historized-raw-data/wyscout/"+entity+"/checkpoint/"
else:
    table_name = entity+"_"+sub_entity
    file_name = entity+"*"
    print(table_name)
    #file_path = source_path + entity + "/" + sub_entity + "/" + date  #+ file_name
    target_path = "/mnt/historized-raw-data/wyscout/"+entity+"_"+sub_entity+"/"
    schema = "/mnt/historized-raw-data/wyscout/"+entity+"_"+sub_entity+"/schema/"
    checkpoint_path = "/mnt/historized-raw-data/wyscout/"+entity+"_"+sub_entity+"/checkpoint/"


# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("mergeSchema","true")
    .option("cloudFiles.format", "parquet") 
    .option("cloudFiles.schemaLocation",schema)
    .option("cloudFiles.includeExistingFiles","false")
    .load(file_path)
    )

# COMMAND ----------

######## Write files into Delta ######
df.writeStream.format("delta").option("checkpointLocation",checkpoint_path).start(target_path).awaitTermination()

# COMMAND ----------

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))

# COMMAND ----------

##### Create external table using delog log files #####


sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1}
                 USING DELTA
                 LOCATION '{2}/{1}/'   
                """.format(BRONZE_CATALOG_NAME,table_name,HISTORIZED_RAW_CONTAINER))

# COMMAND ----------

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

"""%sql
select * from bronze_dev.staging_raw.areas"""

# COMMAND ----------

"""%sql
drop table bronze_dev.staging_raw.areas"""

# COMMAND ----------

dbutils.fs.ls('/mnt/standarized-raw-data/wyscout/areas/2023/04/21/')
