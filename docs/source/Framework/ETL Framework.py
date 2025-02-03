# Databricks notebook source
import json
import time

# COMMAND ----------

########### Init the Environnement Variables ##############################################

GOVERNANCE_CATALOG_NAME=dbutils.secrets.get(scope = "key-vault-scope", key = "GOVERNANCE-CATALOG-NAME")
CONFIG_DATA_PLATEFORM_CONTAINER=dbutils.secrets.get(scope = "key-vault-scope", key = "CONFIG-DATA-PLATEFORM-CONTAINER")
spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")

# COMMAND ----------

# MAGIC %run "./Configuration Management"

# COMMAND ----------

"""
Program to return the path of the current notebook
"""
def get_current_notebook_path():
  return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()


# COMMAND ----------

"""
Program to return the name of the current notebook
"""

def get_current_notebook_name():

  #Get current notebook name
  notebook_path = get_current_notebook_path()

  #Splitted by / to get the notebook name 
  notebook_path_splitted= notebook_path.split("/")

  #get the last element of the list object
  notebook_name=notebook_path_splitted[len(notebook_path_splitted) - 1]

  return notebook_name


# COMMAND ----------

"""
Get  the watermark value for a data source name  
"""

def get_raw_watermark_value(data_provider,data_source_name):
  dataframe=sqlContext.sql("""SELECT raw_watermark_value FROM {0}.config_data_platform.watermarktable
                           where data_provider='{1}' and data_source_name='{2}' 
                           """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
  if dataframe.count()>0:
    return dataframe.collect()[0][0]
  else :
    return "PARAMETER_NOT_FOUND"


# COMMAND ----------

"""
Get  the watermark value for a data source name  
"""

def get_raw_initial_watermark_value(data_provider,data_source_name):
  dataframe=sqlContext.sql("""SELECT raw_initial_watermark_value FROM {0}.config_data_platform.watermarktable
                           where data_provider='{1}' and data_source_name='{2}' 
                           """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
  if dataframe.count()>0:
    return dataframe.collect()[0][0]
  else :
    return "PARAMETER_NOT_FOUND"

# COMMAND ----------

"""
Set the watermark value for a data source name  
"""

def set_raw_watermark_value(data_provider,data_source_name,raw_watermark_value):
  
  num_retries = 0
  max_retries=10
  while True:
        try:
            new_watermark=[(data_provider,data_source_name,raw_watermark_value)]
            column=["data_provider","data_source_name","raw_watermark_value"]
            dataframe=spark.createDataFrame(data=new_watermark,schema=column)
            dataframe.createOrReplaceTempView("v_new_watermark")
            sqlContext.sql("""MERGE INTO {0}.config_data_platform.watermarktable as target USING v_new_watermark as source
                                ON target.data_provider = source.data_provider and target.data_source_name = source.data_source_name
                                WHEN MATCHED THEN UPDATE SET target.raw_watermark_value= source.raw_watermark_value
                            """.format(GOVERNANCE_CATALOG_NAME))
            break
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                time.sleep(120)
                num_retries += 1


# COMMAND ----------

"""
Set the initial watermark value for a data source name  
"""


def set_raw_initial_watermark_value(data_provider,data_source_name,raw_initial_watermark_value):

  num_retries = 0
  max_retries=10
  
  while True:
        try:
            new_watermark=[(data_provider,data_source_name,raw_initial_watermark_value)]
            column=["data_provider","data_source_name","raw_initial_watermark_value"]
            dataframe=spark.createDataFrame(data=new_watermark,schema=column)
            dataframe.createOrReplaceTempView("v_new_watermark")
            sqlContext.sql("""MERGE INTO {0}.config_data_platform.watermarktable as target USING v_new_watermark as source
                                ON target.data_provider = source.data_provider and target.data_source_name = source.data_source_name
                                WHEN MATCHED THEN UPDATE SET target.raw_initial_watermark_value= source.raw_initial_watermark_value
                            """.format(GOVERNANCE_CATALOG_NAME))
            break
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                time.sleep(120)
                num_retries += 1

# COMMAND ----------

"""
Set the initial watermark value for a data source name  
"""


def set_standarize_watermark_value(data_provider,data_source_name,standarize_watermark_value):

  num_retries = 0
  max_retries=10

  while True:
        try:
            new_watermark=[(data_provider,data_source_name,standarize_watermark_value)]
            column=["data_provider","data_source_name","standarize_watermark_value"]
            dataframe=spark.createDataFrame(data=new_watermark,schema=column)
            dataframe.createOrReplaceTempView("v_new_watermark")
            sqlContext.sql("""MERGE INTO {0}.config_data_platform.watermarktable as target USING v_new_watermark as source
                                ON target.data_provider = source.data_provider and target.data_source_name = source.data_source_name
                                WHEN MATCHED THEN UPDATE SET target.standarize_watermark_value= source.standarize_watermark_value
                            """.format(GOVERNANCE_CATALOG_NAME))
            break
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                time.sleep(120)
                num_retries += 1



# COMMAND ----------

"""
Set the initial watermark value for a data source name  
"""


def set_staging_watermark_value(data_provider,data_source_name,staging_watermark_value):
  
  num_retries = 0
  max_retries=10
  while True:
        try:
            new_watermark=[(data_provider,data_source_name,staging_watermark_value)]
            column=["data_provider","data_source_name","staging_watermark_value"]
            dataframe=spark.createDataFrame(data=new_watermark,schema=column)
            dataframe.createOrReplaceTempView("v_new_watermark")
            sqlContext.sql("""MERGE INTO {0}.config_data_platform.watermarktable as target USING v_new_watermark as source
                                ON target.data_provider = source.data_provider and target.data_source_name = source.data_source_name
                                WHEN MATCHED THEN UPDATE SET target.staging_watermark_value= source.staging_watermark_value
                            """.format(GOVERNANCE_CATALOG_NAME))
            break
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                time.sleep(120)
                num_retries += 1   

# COMMAND ----------

"""
Get  the watermark value for a data source name  
"""

def get_standarize_watermark_value(data_provider,data_source_name):
  dataframe=sqlContext.sql("""SELECT standarize_watermark_value FROM {0}.config_data_platform.watermarktable
                           where data_provider='{1}' and data_source_name='{2}' 
                           """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
  if dataframe.count()>0:
    return dataframe.collect()[0][0]
  else :
    return "PARAMETER_NOT_FOUND"

# COMMAND ----------

"""
Check wheter the standarize layer should be processed  
"""

def is_standarize_raw_updated(data_provider,data_source_name):
    dataframe=sqlContext.sql(""" select 1 FROM {0}. config_data_platform.watermarktable
                                 where data_provider='{1}' and data_source_name='{2}' and DATEDIFF( raw_watermark_value, standarize_watermark_value )<=0
                             """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
    if dataframe.count()>0:
        return True
    else :
        return False

# COMMAND ----------

"""
Check wheter the staging layer should be processed  
"""

def is_staging_raw_updated(data_provider,data_source_name):
    dataframe=sqlContext.sql(""" select 1 FROM {0}. config_data_platform.watermarktable
                                 where data_provider='{1}' and data_source_name='{2}' and DATEDIFF( standarize_watermark_value, staging_watermark_value )<=0
                             """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
    if dataframe.count()>0:
        return True
    else :
        return False

# COMMAND ----------

"""
Get the incremental raw path to be processed 
"""

def get_incremental_raw_path(data_provider,data_source_name):
    dataframe=sqlContext.sql(""" select data_provider,
                                 metadata,
                                 YEAR(raw_watermark_value) as year,
                                 MONTH(raw_watermark_value) as month,
                                 DAY(raw_watermark_value) as day
                                 FROM {0}. config_data_platform.watermarktable
                                 where data_provider='{1}' and data_source_name='{2}'
                             """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
    if dataframe.count()>0:
        data_provider=dataframe.collect()[0]["data_provider"]
        metadata=dataframe.collect()[0]["metadata"]
        year=dataframe.collect()[0]["year"]
        month=dataframe.collect()[0]["month"]
        day=dataframe.collect()[0]["day"]
        json_metadata=json.loads(metadata)
        if json_metadata["subentity"]!="":
            return "/mnt/raw/{:s}/{:s}/{:s}/{:04d}/{:02d}/{:02d}/".format(data_provider,json_metadata["entity"],json_metadata["subentity"],year,month,day)
        else:
            return "/mnt/raw/{:s}/{:s}/{:04d}/{:02d}/{:02d}/".format(data_provider,json_metadata["entity"],year,month,day)
    else :
        return "PARAMETER_NOT_FOUND"

# COMMAND ----------

"""
Get the incremental standarizee raw data path to be processed 
"""

def get_incremental_standarized_raw_path(data_provider,data_source_name):
    dataframe=sqlContext.sql(""" select data_provider,
                                 metadata,
                                 YEAR(raw_watermark_value) as year,
                                 MONTH(raw_watermark_value) as month,
                                 DAY(raw_watermark_value) as day
                                 FROM {0}. config_data_platform.watermarktable
                                 where data_provider='{1}' and data_source_name='{2}'
                             """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
    if dataframe.count()>0:
        data_provider=dataframe.collect()[0]["data_provider"]
        metadata=dataframe.collect()[0]["metadata"]
        year=dataframe.collect()[0]["year"]
        month=dataframe.collect()[0]["month"]
        day=dataframe.collect()[0]["day"]
        json_metadata=json.loads(metadata)
        if json_metadata["subentity"]!="":
            return "/mnt/standarized-raw-data/{:s}/{:s}/{:s}/{:04d}/{:02d}/{:02d}/".format(data_provider,json_metadata["entity"],json_metadata["subentity"],year,month,day)
        else:
            return "/mnt/standarized-raw-data/{:s}/{:s}/{:04d}/{:02d}/{:02d}/".format(data_provider,json_metadata["entity"],year,month,day)
    else :
        return "PARAMETER_NOT_FOUND"

# COMMAND ----------

"""
Get the intial raw path to be processed 
"""

def get_initial_raw_path(data_provider,data_source_name):
    dataframe=sqlContext.sql(""" select data_provider,
                                 metadata,
                                 YEAR(raw_initial_watermark_value) as year,
                                 MONTH(raw_initial_watermark_value) as month,
                                 DAY(raw_initial_watermark_value) as day
                                 FROM {0}. config_data_platform.watermarktable
                                 where data_provider='{1}' and data_source_name='{2}'
                             """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
    if dataframe.count()>0:
        data_provider=dataframe.collect()[0]["data_provider"]
        metadata=dataframe.collect()[0]["metadata"]
        year=dataframe.collect()[0]["year"]
        month=dataframe.collect()[0]["month"]
        day=dataframe.collect()[0]["day"]
        json_metadata=json.loads(metadata)
        return "/mnt/raw/{:s}/{:s}/{:s}/{:04d}/{:02d}/{:02d}/".format(data_provider,json_metadata["entity"],json_metadata["subentity"],year,month,day)
    else :
        return "PARAMETER_NOT_FOUND"

# COMMAND ----------

"""
Get the get_standarize_raw_path raw path to be processed 
"""

def get_standarize_raw_path(data_provider,data_source_name):
    dataframe=sqlContext.sql(""" select data_provider,
                                 metadata,
                                 YEAR(standarize_watermark_value) as year,
                                 MONTH(standarize_watermark_value) as month,
                                 DAY(standarize_watermark_value) as day
                                 FROM {0}. config_data_platform.watermarktable
                                 where data_provider='{1}' and data_source_name='{2}'
                             """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
    if dataframe.count()>0:
        data_provider=dataframe.collect()[0]["data_provider"]
        metadata=dataframe.collect()[0]["metadata"]
        year=dataframe.collect()[0]["year"]
        month=dataframe.collect()[0]["month"]
        day=dataframe.collect()[0]["day"]
        json_metadata=json.loads(metadata)
        return "/mnt/standarized-raw-data/{:s}/{:s}/{:s}/{:04d}/{:02d}/{:02d}/".format(data_provider,json_metadata["entity"],json_metadata["subentity"],year,month,day)
    else :
        return "PARAMETER_NOT_FOUND"

# COMMAND ----------

"""
Get  the watermark value for a data source name  
"""

def get_staging_watermark_value(data_provider,data_source_name):
  dataframe=sqlContext.sql("""SELECT staging_watermark_value FROM {0}.config_data_platform.watermarktable
                           where data_provider='{1}' and data_source_name='{2}' 
                           """.format(GOVERNANCE_CATALOG_NAME,data_provider,data_source_name))
  if dataframe.count()>0:
    return dataframe.collect()[0][0]
  else :
    return "PARAMETER_NOT_FOUND"

# COMMAND ----------

"""
Init a new Data Source Name 
"""
def init_new_data_source(data_provider,data_source_name,metadata):
  num_retries = 0
  max_retries=-1
  while True:
        try:
            new_watermark=[(data_provider,data_source_name,'1900-01-01','1900-01-01',True,metadata,'1900-01-01','1900-01-01')]
            column=["data_provider","data_source_name","raw_watermark_value","raw_initial_watermark_value","is_active","metadata","standarize_watermark_value","staging_watermark_value"]
            dataframe=spark.createDataFrame(data=new_watermark,schema=column)
            dataframe.createOrReplaceTempView("v_new_watermark")
            sqlContext.sql("""MERGE INTO {0}.config_data_platform.watermarktable as target USING v_new_watermark as source
                                ON target.data_provider = source.data_provider and target.data_source_name = source.data_source_name
                                WHEN NOT MATCHED THEN INSERT * 
                            """.format(GOVERNANCE_CATALOG_NAME))
            break
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                time.sleep(120)
                num_retries += 1   

# COMMAND ----------


#Example of the function call 


#set_raw_watermark_value('wyscout','matches_events','2023-04-17')

#set_raw_initial_watermark_value('wyscout','matches_events','2023-04-17')

#get_raw_initial_watermark_value('wyscout','matches_events')

#set_standarize_watermark_value('wyscout','matches_events','2023-04-16')

#get_standarize_watermark_value('wyscout','matches_events')

#set_staging_watermark_value('wyscout','matches_events','2023-04-15')

#get_staging_watermark_value('wyscout','matches_events')

#if(is_standarize_raw_updated('wyscout','matches_events')==False):
    #print ('Process the standarization Layer ')

#if(is_staging_raw_updated('wyscout','matches_events')==False):
    #print ('Process the staging Layer ')

#print(get_standarize_raw_path('wyscout','matches_events'))

#print(get_initial_raw_path('wyscout','matches_events'))

#print(get_incremental_raw_path('wyscout','areas'))



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_dev.standarized_raw.wyscout_players_details 
# MAGIC WHERE firstName = 'Hugo' AND lastName = 'Siquet' OR firstName = 'Ritchie ' and lastName = 'De Laet'  OR firstName = 'Marlon' and lastName = 'Fossey'
# MAGIC ORDER BY firstName
