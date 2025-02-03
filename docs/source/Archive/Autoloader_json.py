# Databricks notebook source
#from pyspark.sql.functions import *
#from pyspark.sql.types import * 
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, count

# COMMAND ----------

#DIRECTORIES

root_directory = "/mnt/"
in_dir = "raw/wyscouttmp/"
out_dir = "standarized-raw-data/wyscouttmp/"

# Datatype
entity = "competitions"
subentity = "areas"

#path to get into
date = "/2023/04/21/"

# COMMAND ----------

################################################################
#------------------------- Autoloader -----------------------#
################################################################

# CHEMIN DU DIRECTORY
# print("------------------------ the path of the folder is ------------------> " + path)
path = root_directory + in_dir + entity + "/" + subentity + date
df1 = spark.read.option("multiline","true").json(path)
Schema = df1.schema

#schema = "/mnt/raw/wyscout/competitions/areas/2023/04/25/schema/"

out_schema = "/mnt/standarized-raw-data/wyscouttmp/competitions/areas/2023/04/21/schema/"
out_path = "/mnt/standarized-raw-data/wyscouttmp/competitions/areas/2023/04/21/"
table_name = 'matches_event'

'''files_to_treat = dbutils.fs.ls(path)
display(files_to_treat)'''


# COMMAND ----------

def get_stream():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .schema(Schema)
            #.option("cloudFiles.schemaLocation",schema) 
            .load(path)
    )

def doSum (f):
    display(f)

display(get_stream())
stream = get_stream()

'''count_df = (stream.withColumn("file", input_file_name()) 
            .groupBy("file")
            .agg(count("*")) )
            
#display(count_df)
count_df.display()'''

out_for_batch = "/mnt/standarized-raw-data/wyscouttmp/competitions/areas/"

def runThis(df, batchId):
    df.display()
    '''(df.write
        .save(out_for_batch))'''
    
output = (stream.writeStream.format("json")
    #.trigger(once=True)
    #.outputMode("append")
    #.foreachBatch(runThis)
    #.queryName("Fetch data from landing to curated")
    .option('checkpointLocation', out_schema)
    .start(out_path))

# COMMAND ----------

'''output.recentProgress

df_out = spark.read.format('delta').load(out_path)

print(df_out)

display(df_out)

#df_out.write.format("delta").parquet(root_directory + out_dir + entity  + "/" + subentity +  date + "test_parquet/") '''

