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
from pyspark.sql.session import SparkSession

from pyspark.sql import DataFrame as SDF


# COMMAND ----------

directory = "/mnt/raw/wyscout/"
std_directory = "/mnt/standarized-raw-data/wyscout/"
coaches_path = directory + "/coaches/coaches/2023/04/21/*.json"
matches_path = directory + "/matches/events/2023/04/21/*.json"
competitions_players_path = directory + "/competitions/players/2023/04/21/*.json"

df = spark.read.option("multiline","true").json(competitions_players_path)
#df.printSchema()
#df.show()

# Read all JSON files from a folder
#df3 = spark.read.json("resources/*.json")
#df3.show()

# COMMAND ----------

from typing import Dict, Tuple, Final

def rename_dataframe_cols (df: SDF, col_names: Dict[str, str]) -> SDF:
  return df.select(*[col(col_names).alias(col_names.get(col_name, col_name)) for col_name in df.columns])

def update_column_names(df: SDF, index: int) -> SDF:
  df_temp = df
  all_cols = df_temp.columns
  print(all_cols)
  new_cols = dict((column, f"{column}*{index}") for column in all_cols)

  return df_temp

def flatten_json(df_arg: SDF, index: int = 1) -> SDF:
  """flatern json in spark"""
  #update all column names with index 1
  df = update_column_names(df_arg, index) if index == 1 else df_arg

  #Get all field names from the dataframe
  fields = df.schema.fields

  #For all columns in the dataframe
  for field in fields:
    data_type = str(field.dataType)
    column_name = field.name

    first_10_chars = data_type[0:10]

    # If it is an array column

    if first_10_chars == 'ArrayType':
      #explode array column
      df_temp = df.withColumn(column_name, explode_outer(col(column_name)))
      return flatten_json(df_temp, index + 1)
    
    #If it is a json object
    elif first_10_chars == 'StructType':
      current_col = column_name

      append_str = current_col

      #Get data type from current column

      data_type_str = str(df.schema[current_col].dataType)

      #Change the column name if the current name exists in the data type string

      df_temp = df.withColumnRenamed(column_name, column_name + "#1") \
        if column_name in data_type_str else df
      current_col = current_col + "#1" if column_name in data_type_str else current_col

      # Expand struct column values

      df_before_expanding = df_temp.select(f"{current_col}.*")
      newly_gen_cols = df_before_expanding.columns

      #Find next level value for the column

      begin_index = append_str.rfind('*')

      end_index = len(append_str)
      level = append_str[begin_index + 1: end_index]
      print(level)
      next_level = int(level) + 1

      # Update column names with new level
      custom_cols = dict((field, f"{append_str}->{next_level}") for field in newly_gen_cols)
      df_temp2 = df_temp.select("*", f"{current_col}.*").drop(current_col)
      df_temp3 = df_temp2.transform(lambda df_x: rename_dataframe_cols(df_x, custom_cols))
      return flatten_json(df_temp3, index + 1)
    
    return df




# COMMAND ----------

################## Initiate the Spark Session #########################

if __name__ == "__main__":
  spark = SparkSession \
    .builder \
      .appName("FlatJson") \
      .master("local[*]").getOrCreate()

  sc = spark.sparkContext
  #tsRDD = sc.parallelize(competitions_players_path)

  df2 = spark.read.json(matches_path)
  
  print("---------------------- df2 ------------------------")
  df2.show()
  #df2.printSchema()
  df3 = flatten_json(df2)
  df_col = df3.columns
  
  for f in df_col:
    df3.select(f).show()
    flatten_json(df3.select(f))
  
