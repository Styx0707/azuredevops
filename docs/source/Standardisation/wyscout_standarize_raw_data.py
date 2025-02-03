# Databricks notebook source
# MAGIC %run "../Framework/ETL Framework" 

# COMMAND ----------

# MAGIC %run "../Framework/Logging Management"

# COMMAND ----------

# MAGIC %run "../Framework/File Management"

# COMMAND ----------

from pyspark.sql import types 
import pyspark.sql.functions as functions
from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql.functions import col, lit
dbutils.widgets.text("data_source_name","")


# COMMAND ----------

################################################################
#------------------------- Flatten JSON -----------------------#
################################################################

def flatten(df):

    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, types.ArrayType) or isinstance(field.dataType, types.StructType)
    ])

    if(list(complex_fields.keys()) == []):
        return df

    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], types.StructType):
            expanded = [functions.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], types.ArrayType): 
            df = df.withColumn(col_name, functions.explode_outer(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, types.ArrayType) or isinstance(field.dataType, types.StructType)
        ])
        
    '''for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))'''

    return df
    

# COMMAND ----------

def wyscout_standarized_raw(data_source_name):
    
    BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
    
    STANDARIZED_RAW_CONTAINER=get_parameter("GLOBAL","STANDARIZED_RAW_CONTAINER")
    
    data_provider="wyscout"

    target_table=data_provider+"_"+data_source_name

    temporary_view="v_"+target_table
    
    incremental_raw_path=get_incremental_raw_path(data_provider,data_source_name)

    raw_watermark_value=get_raw_watermark_value(data_provider,data_source_name)

    file_exist = is_file_exist(incremental_raw_path)

    if(file_exist == True):

        raw_dataframe = spark.read.option("multiline","true").json(incremental_raw_path+"*.json")

        standarize_dataframe = flatten(raw_dataframe)

        print (standarize_dataframe.schema)

        display(standarize_dataframe)

        standarize_dataframe=standarize_dataframe.withColumn("META_UNVALIDATED",lit("false").cast("boolean"))\
                                         .withColumn('META_EXTRACTION_DATE',lit(raw_watermark_value).cast("TIMESTAMP"))\
                                         .withColumn('META_LOAD_DATE',lit(functions.current_timestamp()).cast("TIMESTAMP"))\
                                         .withColumn('META_EXTRACTION_DATE_YEAR',year(lit(raw_watermark_value)).cast("INTEGER"))\
                                         .withColumn('META_EXTRACTION_DATE_MONTH',month(lit(raw_watermark_value)).cast("INTEGER"))\
                                         .withColumn('META_EXTRACTION_DATE_DAY',dayofmonth(lit(raw_watermark_value)).cast("INTEGER"))\
                                             
        standarize_dataframe.createOrReplaceTempView(temporary_view)

        standarize_dataframe.printSchema()

        sqlContext.sql("""insert into {0}.standarized_raw.{1} select * from {2} """.format(BRONZE_CATALOG_NAME,target_table,temporary_view))

    

# COMMAND ----------

data_source_name=dbutils.widgets.get("data_source_name")

wyscout_standarized_raw(data_source_name)
    

