# Databricks notebook source
import sys
import json
import datetime
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import types as T
import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from datetime import datetime


# COMMAND ----------

################################################################
#------------------------- Path parameter ---------------------#
################################################################

#DIRECTORIES
root_directory = "/mnt/"
in_dir = "raw/wyscouttmp/"
out_dir = "standarized-raw-data/wyscout/"

# Datatype
entity = "competitions"
subentity = "areas"

#path to get into
date = "/2023/04/21/"

# CHEMIN DU DIRECTORY
path = root_directory + in_dir + entity + "/" + subentity + date
print("------------------------ the path of the folder is ------------------> " + path)

# COMMAND ----------

################################################################
#------------------------- Flatten JSON -----------------------#
################################################################

def flatten(df):

    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])

    if(list(complex_fields.keys()) == []):
        return df

    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode_outer(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])
        
    '''for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))'''

    return df
    


# COMMAND ----------

# Clean output repo
PATH = root_directory + out_dir + entity  + "/" + subentity + "/"
dbutils.fs.rm(PATH,True)

#display(dbutils.fs.ls(path))
files_to_treat = dbutils.fs.ls(path)

for f in files_to_treat:
    '''display(f.path)
    display(f.name)'''
    new_fn = f.name.rsplit( ".", 1 )[ 0 ]
    df1 = spark.read.option("multiline","true").json(f.path)

    if df1.isEmpty():
        print("The DataFrame is empty.")
    else:
        print("The DataFrame is not empty.")
        print(df1)
        df1.printSchema()
        fl_df = flatten(df1)
        
        #print("------------------------ the schema of the flattened dataframe : ------------------" )
        fl_df.printSchema()

        # Write the parquet file into the designed folder

        fl_df.write.parquet(root_directory + out_dir + entity  + "/" + subentity +  date + new_fn)  
        parDF=spark.read.parquet(root_directory + out_dir + entity  + "/" + subentity + date + new_fn)
        display(parDF)

    



# COMMAND ----------

# MAGIC %sql
# MAGIC update governance_dev.config_data_platform.watermarktable 
# MAGIC set raw_watermark_value='2023-05-08'
# MAGIC where data_source_name='matches_formations'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from governance_dev.config_data_platform.watermarktable
# MAGIC where data_source_name='matches_players_advancedstats'

# COMMAND ----------

# MAGIC %sql
# MAGIC update governance_dev.config_data_platform.watermarktable 
# MAGIC set metadata='{"entity":"matches", "subentity":"players_advancedstats"}'
# MAGIC where data_source_name='matches_players_advancedstats'

# COMMAND ----------

# MAGIC %sql
# MAGIC update governance_dev.config_data_platform.watermarktable 
# MAGIC set data_source_name='matches_players_advancedstats'
# MAGIC where data_source_name='matches_player_advancedstats'

# COMMAND ----------


