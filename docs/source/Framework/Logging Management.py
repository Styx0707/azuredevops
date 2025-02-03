# Databricks notebook source
#%run "./ETL Framework"

# COMMAND ----------

#%run "./Configuration Management"

# COMMAND ----------

########### Init the Environnement Variables ##############################################

GOVERNANCE_CATALOG_NAME=dbutils.secrets.get(scope = "key-vault-scope", key = "GOVERNANCE-CATALOG-NAME")
CONFIG_DATA_PLATEFORM_CONTAINER=dbutils.secrets.get(scope = "key-vault-scope", key = "CONFIG-DATA-PLATEFORM-CONTAINER")
spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")

# COMMAND ----------

import logging
import time
from datetime import datetime 
import pytz
import json


# COMMAND ----------

def logger_init(logger_name):
  """ 
  logger_name such as NOTEBOOK."Name of the notebook"
  This Function initiate the logger 
  """
  
  ##Defining local directory and file name pattern
  p_dir="/tmp/"
  p_filename='logs_'+logger_name+".log"
  p_logfile=p_dir+p_filename
  
  ##Setting the Logger parameter

  logger=logging.getLogger(logger_name)
  
  #logger.setLevel(logging.DEBUG)

  #fomatter=logging.Formatter('%(asctime)s ; %(name)s ; %(levelname)s ; %(message)s')
  
  #filehandler=logging.FileHandler(p_logfile,mode='a')

  #filehandler.setFormatter(fomatter)

  #logger.addHandler(filehandler)

  return logger
  




# COMMAND ----------

def logger_close(logger):
  """ 
  logger_name such as NOTEBOOK."Name of the notebook"
  This Function initiate the logger 
  """
  ''' 
  handlers = logger.handlers[:]
  for handler in handlers:
    handler.close()
    logger.removeHandler(handler)
  
    ##Defining local directory and file name pattern
    ##Defining local directory and file name pattern
    
  p_dir="/tmp/"
  local_filename='logs_'+logger.name+".log"
  p_logfile=p_dir+local_filename
  cloud_filename='logs_'+logger.name+".log"
  #print(cloud_filename)

  # Copy the log file generated into the dbfs file

  partitions=datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y/%m/%d/')

  dbutils.fs.mv("file:"+p_logfile,"/mnt/admin/logs/"+partitions+cloud_filename)
  '''


# COMMAND ----------

# Example of Using the Logging management framework


##### Get the current notebook name
#notebook_name = get_current_notebook_name()

######Init logger 
#logger=logger_init(notebook_name)

#######Logging some messages
#logger.debug("--------------------Start INFO Logger here ----------------------------------")
#logger.info("--------------------Start INFO Logger here ----------------------------------")
#logger.warning("--------------------Start Warning Logger here ----------------------------------")
#logger.error("--------------------Start Error Logger here ----------------------------------")
#logger.critical("--------------------Start Error Logger here ----------------------------------")

#dictionnary_message=dict(
#  event_log="ProcessingStarted",
#  time_started=datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M'),
#  message="Starting calling Season/Players API ",
#  row_count="50"
#)

#logging_message=json.dumps(dictionnary_message)

#logger.info(logging_message)


#Close logger
#logger_close(logger)




# COMMAND ----------

sqlContext.sql("""
CREATE TABLE IF NOT EXISTS LOGGING
(
  logging_time string ,
  logging_program string,
  logging_level string,
  logging_data string
)
USING CSV OPTIONS(path '/mnt/admin/logs/*/*/*/*',header=false,delimiter=';')
""".format(GOVERNANCE_CATALOG_NAME,CONFIG_DATA_PLATEFORM_CONTAINER))




# COMMAND ----------

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.config_data_platform.logging(
                  parameter_scope STRING,
                  parameter_key STRING,
                  parameter_value STRING
                  )
                  USING DELTA
                  LOCATION '{1}/logs/'   
                """.format(GOVERNANCE_CATALOG_NAME,CONFIG_DATA_PLATEFORM_CONTAINER))

# COMMAND ----------

"""
  Refreshing the principal logging table 
  
  to use only when the consulting logs is needed

"""

def refresh_logging_table():
  sqlContext.sql("""REFRESH TABLE LOGGING""")
  sqlContext.sql("""truncate table {0}.config_data_platform.logging""".format(GOVERNANCE_CATALOG_NAME))
  sqlContext.sql("""insert into {0}.config_data_platform.logging select * from LOGGING""".format(GOVERNANCE_CATALOG_NAME))
  

# COMMAND ----------

#refresh_logging_table()

# COMMAND ----------

#example of consulting logs
#%sql
#select* from governance_prd.config_data_platform.logging


# COMMAND ----------

##### Get the current notebook name
#notebook_name = get_current_notebook_name()

######Init logger 
#logger=logger_init(notebook_name)

#######Logging some messages
#logger.debug("--------------------Start INFO Logger here ----------------------------------")
#logger.info("--------------------Start INFO Logger here ----------------------------------")
#logger.warning("--------------------Start Warning Logger here ----------------------------------")
#logger.error("--------------------Start Error Logger here ----------------------------------")
#logger.critical("--------------------Start Error Logger here ----------------------------------")

#dictionnary_message=dict(
  #event_log="ProcessingStarted",
  #time_started=datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M'),
  #message="Starting calling Season/Players API ",
  #row_count="50"
#)

#logging_message=json.dumps(dictionnary_message)

#logger.info(logging_message)



#logger_close(logger)


