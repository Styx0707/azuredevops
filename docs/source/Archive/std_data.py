# Databricks notebook source
###########################################################
#------------------------- IMPORTS -----------------------#
###########################################################

import sys
import json
import datetime
import pandas as pd

from pyspark.sql.functions import *
from pyspark.sql.types import * 
from pyspark.sql import SparkSession

from pyspark.sql import DataFrame

# COMMAND ----------

directory = "/mnt/raw/corporate/wyscout/"
std_directory = "/mnt/standarized-raw-data/wyscout/"
coaches_path = directory + "/coaches/coaches/2023/04/06/*.json"
matches_path = directory + "/matches/events/2023/04/06/*.json"
competitions_path = directory + "/competitions/competitions/2023/04/06/*.json"

df = spark.read.option("multiline","true").json(competitions_path)
df.printSchema()
df.show()

# Read all JSON files from a folder
#df3 = spark.read.json("resources/*.json")
#df3.show()

# COMMAND ----------

### Convert the data frame to pandas
#flattened_df = df.select(explode("areas").alias("area"))

#flattened_df.show(truncate=False)

# Create outer method to return the flattened Data Frame
def flatten_json_df(_df: DataFrame) -> DataFrame:
    # List to hold the dynamically generated column names
    flattened_col_list = []
    
    # Inner method to iterate over Data Frame to generate the column list
    def get_flattened_cols(df: DataFrame, struct_col: str = None) -> None:
        for col in df.columns:
            if df.schema[col].dataType.typeName() != 'struct':
                if struct_col is None:
                    flattened_col_list.append(f"{col} as {col.replace('.','_')}")
                else:
                    t = struct_col + "." + col
                    flattened_col_list.append(f"{t} as {t.replace('.','_')}")
            else:
                chained_col = struct_col +"."+ col if struct_col is not None else col
                get_flattened_cols(df.select(col+".*"), chained_col)
    
    # Call the inner Method
    get_flattened_cols(_df)
    
    # Return the flattened Data Frame
    return _df.selectExpr(flattened_col_list)




# show the flattened data
#flattened_areas.show(truncate=False)

### USE fonciton

# Generate the flattened DF
flattened_df = flatten_json_df(df)
flattened_df.show(10)
# Print Schema
flattened_df.printSchema()


# COMMAND ----------

# Write the parquet file into the designed folder

# creer la date du jour + parametrer pour le dossier...

flattened_df.write.parquet(std_directory + "/competitions/competitions/2023/04/06/competitions_195_2023_04_06")

parDF=spark.read.parquet(std_directory + "/competitions/competitions/2023/04/06/competitions_195_2023_04_06")

parDF.show()
