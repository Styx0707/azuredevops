# Databricks notebook source
import sys
import os
import os.path
import requests
import json
import io
import re
import base64
import time
from datetime import datetime

# COMMAND ----------

###########################################################
#############Function : is_file_exist #####################
# is_file_exist return True if file exist                ##
# return false if file exist                             ##
###########################################################
###########################################################

def is_file_exist(file_path): 
  try: 
    dbutils.fs.ls(file_path)
    return True
  except: 
    return False

###########################################################
###########################################################

# COMMAND ----------

def check_file_exist(file_name):
    if "?" in file_name:
        file_name = file_name.replace("?", "=" )
    try: 
      dbutils.fs.ls(file_name)
      return True
    except: 
      return False

# COMMAND ----------

#####################################################################
#############Function : get_local_json_file##########################
# get_local_json_file to open every json that should               ##
# be processed locally (Example : Python)                          ##
# Since DBR 7.x it's not possible to open file directly from DBFS  ##
#####################################################################
#####################################################################
def get_local_json_file(file_name):
    if "?" in file_name:
        file_name = file_name.replace("?", "=" )
    
    #Copying from DBFS to the Local
    dbfs_path = 'dbfs:'+file_name
    local_path = 'file://'+file_name
    dbutils.fs.cp(dbfs_path, local_path)

    #Get the Json content and remove the local file
    f = open(file_name)
    data = json.load(f)
    dbutils.fs.rm(local_path)
    return data

# COMMAND ----------


