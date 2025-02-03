# Databricks notebook source
# MAGIC %run "../Ingestion/Wyscout_function"

# COMMAND ----------

dbutils.widgets.text("data_source_name","")
dbutils.widgets.text("query_string","")
dbutils.widgets.text("obj_type","")
dbutils.widgets.text("gender_type","")

# COMMAND ----------

###########################################################
#-------------------------SEARCH--------------------------#
###########################################################
def wyscout_search(logger, query_string, obj_type, gender_type):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of searched data")

    try:
      #################################################
      #--------------Configuration file---------------#
      #################################################
      config_file = get_set_properties(logger)
      
      #################################################
      #-----------------Authorization-----------------#
      #################################################
      headers = get_headers_authorization(config_file, logger)

      #################################################
      #----------------Search request-----------------#
      #################################################
      search_request(config_file, headers, query_string, obj_type, gender_type, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of searched data completed")

    except Exception as e:
      log_critical_error_process(logger, e)
    
###########################################################

# COMMAND ----------

"""
query_string = "gama" #search string (name of the player, team, ...)
obj_type = "player" #competition / team / player / referee
gender_type = "men" #men / women / (leave blank search both)
"""

time_now = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y_%m_%d_%H_%M_%S')

data_source_name=dbutils.widgets.get("data_source_name")
query_string=dbutils.widgets.get("query_string")
obj_type=dbutils.widgets.get("obj_type")
gender_type=dbutils.widgets.get("gender_type")

logger = logger_init(get_current_notebook_name()+"_"+data_source_name+"_"+time_now)

try :
    #################################################
    #--------------------LEVEL 0--------------------#
    #################################################
    if(data_source_name=='search'):
        wyscout_search(logger, query_string, obj_type, gender_type)

except Exception as e:
    log_critical_error_process(logger, e)
    logger_close(logger)
#################################################

logger_close(logger)
