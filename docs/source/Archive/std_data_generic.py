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
path = directory + "/seasons/career/2023/04/21/*.json"

df1 = spark.read.option("multiline","true").json(path)
df1.printSchema()
df1.show()

def fl_cols(_df):

  # List to hold the dynamically generated column names
  flattened_col_list = []

  def get_flattened_cols(df: DataFrame, struct_col: str = None) -> None:
        for col in df.columns:
            if df.schema[col].dataType.typeName() == 'array':
              print("ce noeud est un array : " + col)
              #df_new = df.select(flatten(col))
              print("------------ Version struct --------------") 
              df_new = df.withColumn(col, explode_outer(col))
              df_new.show()
              #df_new.printSchema()
              chained_col = struct_col +"."+ col if struct_col is not None else col
              #print(chained_col)
              print("------------ Etat de la liste --------------") 
              print(flattened_col_list)
              get_flattened_cols(df_new, chained_col)  
              print(flattened_col_list)
              #fl_cols(df_new)
            elif df.schema[col].dataType.typeName() == 'struct':
                print("ce noeud est une struct : " + col)
                chained_col = struct_col +"."+ col if struct_col is not None else col
                get_flattened_cols(df.select(col+".*"), chained_col)   
            else:
                print("ce noeud est un noeud : " + col)
                print(flattened_col_list)
                if struct_col is None:
                    flattened_col_list.append(f"{col} as {col.replace('.','_')}")
                else:
                    t = struct_col + "." + col
                    flattened_col_list.append(f"{t} as {t.replace('.','_')}")
  
  # Call the inner Method
  get_flattened_cols(_df)   
  
  return _df.selectExpr(flattened_col_list)

        
fl_df = fl_cols(df1)
#fl_df.printSchema()
#fl_df.show()


# COMMAND ----------

### Convert the data frame to pandas
###flattened_df = df.select(explode("rounds").alias("rounds"))

#flattened_df.show(truncate=True)

# Create outer method to return the flattened Data Frame
'''def flatten_json_df(_df: DataFrame) -> DataFrame:
    # List to hold the dynamically generated column names
    flattened_col_list = []

    # Inner method to iterate over Data Frame to generate the column list
    def get_flattened_cols(df: DataFrame, struct_col: str = None) -> None:
        array_bool = True
        while(array_bool == True):
          array_bool = False
          for col in df.columns:
              print(col)
              if df.schema[col].dataType.typeName() == 'array':
                  df = df.select(explode(col).alias(col))
                  df.printSchema()
                  array_bool = True
          for col in new_df.columns:
            if new_df.schema[col].dataType.typeName() != 'struct':
                if struct_col is None:
                    flattened_col_list.append(f"{col} as {col.replace('.','_')}")
                else:
                    t = struct_col + "." + col
                    flattened_col_list.append(f"{t} as {t.replace('.','_')}")
            else:
                chained_col = struct_col +"."+ col if struct_col is not None else col
                get_flattened_cols(new_df.select(col+".*"), chained_col)
                
    # Call the inner Method
    get_flattened_cols(_df)

    # Return the flattened Data Frame
    return _df.selectExpr(flattened_col_list)'''

# show the flattened data
#flattened_df.show(truncate=True)

### USE fonciton

# Generate the flattened DF     
#fdf = flatten_json_df(df)
#fdf.show(10)
# Print Schema
#fdf.printSchema()


# COMMAND ----------

# Write the parquet file into the designed folder

# creer la date du jour + parametrer pour le dossier...

# rajouter supprimer les fichiers TODO

#flattened_df.write.parquet("/mnt/standarized-raw-data/wyscout/areas/2023/04/07/areas.parquet")

#parDF=spark.read.parquet("/mnt/standarized-raw-data/wyscout/areas/2023/04/07/areas.parquet")

#parDF.show()


