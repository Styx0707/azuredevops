# Databricks notebook source
# MAGIC %run "../Framework/Logging Management"

# COMMAND ----------

# MAGIC %run "../Framework/ETL Framework"

# COMMAND ----------

###########################################################
#--------------------IMPORT RESOURCES---------------------#
###########################################################
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
from datetime import timedelta  
import pyspark
import traceback
import pytz #datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M-%S')
###########################################################

# COMMAND ----------

###########################################################
#---------------------Function LOGGING--------------------#
###########################################################

###########################################################
# Function log_starting_process                           #
# Return nothing                                          #
# Log an info that say the process has started            #
###########################################################
def log_starting_process(logger, log_message):
  dict_message = dict(
  event_log = "ProcessingStarted",
  time = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M-%S'),
  message = log_message)
  json_message = json.dumps(dict_message)
  #logger.info(json_message)
###########################################################

###########################################################
# Function log_finished_process                           #
# Return nothing                                          #
# Log an info that say the process has finished           #
###########################################################
def log_finished_process(logger, log_message):
  dict_message = dict(
  event_log = "ProcessingFinished",
  time = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M-%S'),
  message = log_message)
  json_message = json.dumps(dict_message)
  #logger.info(json_message)
###########################################################

###########################################################
# Function log_warning_process                            #
# Return nothing                                          #
# Log a warning that say there is a warning the process   #
###########################################################
def log_warning_process(logger, log_warning):
  dict_message = dict(
  event_log = "ProcessingWarning",
  time = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M-%S'),
  message = log_warning)
  json_message = json.dumps(dict_message)
  #logger.warning(json_message)
###########################################################

###########################################################
# Function log_error_process                     #
# Return nothing                                          #
# Log an error in the process that make crash the script  #
# Close the logger                                        #
###########################################################
def log_error_process(logger, log_error):
  dict_message = dict(
  event_log = "ProcessingError",
  time = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M-%S'),
  message = log_error)
  json_message = json.dumps(dict_message)
  #logger.error(json_message)
###########################################################

###########################################################
# Function log_critical_error_process                     #
# Return nothing                                          #
# Log an error in the process that make crash the script  #
# Close the logger                                        #
###########################################################
def log_critical_error_process(logger, log_critical_error):
  dict_message = dict(
  event_log = "ProcessingCriticalError",
  time = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d-%H-%M-%S'),
  message = log_critical_error)
  json_message = json.dumps(dict_message)
  #logger.critical(json_message)
  #logger_close(logger)
###########################################################

# COMMAND ----------

###########################################################
#-------------------Function PROPERTIES-------------------#
###########################################################

###########################################################
# Function init_properties                                #
# Return nothing                                          #
# Set all the properties needed in the properties table   #
###########################################################
def init_properties(logger):
  try:
    log_starting_process(logger, "Starting initialization of properties")
    
    parameter_key = ["transfer_start_year",
    "file_path",
    "authorization",
    "search_api_url",
    "areas_url_api",
    "list_competitions_url_api",
    "competitions_url_api",
    "competitions_players_url_api",
    "competitions_url_api_matches",
    "competitions_url_api_seasons",
    "competitions_url_api_teams",
    "seasons_url_api",
    "seasons_players_url_api",
    "seasons_transfers",
    "seasons_url_api_assistmen",
    "seasons_url_api_career",
    "seasons_url_api_fixtures",
    "seasons_url_api_matches",
    "seasons_url_api_scorers",
    "seasons_url_api_standings",
    "seasons_url_api_teams",
    "matches_url_api",
    "matches_url_api_formations",
    "matches_url_api_events",
    "matches_url_api_advancedstats",
    "matches_url_api_playeradvancedstats",
    "matches_url_api_directions",
    "matches_url_api_fulltracking",
    "teams_url_api",
    "teams_advanced_stats_url_api",
    "teams_transfers_url_api",
    "teams_url_api_career",
    "teams_url_api_fixtures",
    "teams_url_api_matches",
    "teams_url_api_squad",
    "players_url_api",
    "players_advanced_stats_url_api",
    "players_url_api_career",
    "players_url_api_contractinfo",
    "players_url_api_fixtures",
    "players_url_api_matches",
    "players_url_api_transfers",
    "rounds_url_api",
    "referees_url_api",
    "coaches_url_api",
    "player_match_advanced_stats_url_api",
    "team_match_advanced_stats_url_api",
    "videos_url_api",
    "videos_url_api_qualities",
    "videos_url_api_offsets"]

    parameter_value = ["2000","/mnt/raw/wyscout/",
    "n5pz6r5-cd768rp66-f0bohsk-frtxybhi2j:c,R#tB$u=&XAPMO(Cf4N%tHHP9vXuf",
    "https://apirest.wyscout.com/v4/search?query=query_string&objType=obj_type&gender=gender_type",
    "https://apirest.wyscout.com/v4/areas",
    "https://apirest.wyscout.com/v4/competitions?areaId=",
    "https://apirest.wyscout.com/v4/competitions/",
    "https://apirest.wyscout.com/v4/competitions/wyId/players?limit=100&page=",
    "https://apirest.wyscout.com/v4/competitions/wyId/matches",
    "https://apirest.wyscout.com/v4/competitions/wyId/seasons",
    "https://apirest.wyscout.com/v4/competitions/wyId/teams",
    "https://apirest.wyscout.com/v4/seasons/",
    "https://apirest.wyscout.com/v4/seasons/wyId/players?limit=100&page=",
    "https://apirest.wyscout.com/v4/seasons/wyId/transfers?fromDate=date_from;toDate=date_to",
    "https://apirest.wyscout.com/v4/seasons/wyId/assistmen",
    "https://apirest.wyscout.com/v4/seasons/wyId/career",
    "https://apirest.wyscout.com/v4/seasons/wyId/fixtures",
    "https://apirest.wyscout.com/v4/seasons/wyId/matches",
    "https://apirest.wyscout.com/v4/seasons/wyId/scorers",
    "https://apirest.wyscout.com/v4/seasons/wyId/standings",
    "https://apirest.wyscout.com/v4/seasons/wyId/teams",
    "https://apirest.wyscout.com/v4/matches/wyId?useSides=1",#?useSides=1
    "https://apirest.wyscout.com/v4/matches/wyId/formations",
    "https://apirest.wyscout.com/v4/matches/wyId/events",
    "https://apirest.wyscout.com/v4/matches/wyId/advancedstats?useSides=1",#?useSides=1
    "https://apirest.wyscout.com/v4/matches/wyId/advancedstats/players",
    "https://apirest.wyscout.com/v4/matches/wyId/directions",
    "https://apirest.wyscout.com/v4/matches/wyId/fulltracking",
    "https://apirest.wyscout.com/v4/teams/",
    "https://apirest.wyscout.com/v4/teams/wyId/advancedstats?compId=",
    "https://apirest.wyscout.com/v4/teams/wyId/transfers?fromDate=date_from;toDate=date_to",
    "https://apirest.wyscout.com/v4/teams/wyId/career",
    "https://apirest.wyscout.com/v4/teams/wyId/fixtures",
    "https://apirest.wyscout.com/v4/teams/wyId/matches",
    "https://apirest.wyscout.com/v4/teams/wyId/squad",
    "https://apirest.wyscout.com/v4/players/",
    "https://apirest.wyscout.com/v4/players/wyId/advancedstats?compId=",
    "https://apirest.wyscout.com/v4/players/wyId/career",
    "https://apirest.wyscout.com/v4/players/wyId/contractinfo",
    "https://apirest.wyscout.com/v4/players/wyId/fixtures",
    "https://apirest.wyscout.com/v4/players/wyId/matches",
    "https://apirest.wyscout.com/v4/players/wyId/transfers",
    "https://apirest.wyscout.com/v4/rounds/",
    "https://apirest.wyscout.com/v4/referees/",
    "https://apirest.wyscout.com/v4/coaches/",
    "https://apirest.wyscout.com/v4/players/wyId/matches/matchWyId/advancedstats",
    "https://apirest.wyscout.com/v4/teams/wyId/matches/matchWyId/advancedstats",
    "https://apirest.wyscout.com/v4/videos/",
    "https://apirest.wyscout.com/v4/videos/wyId/qualities",
    "https://apirest.wyscout.com/v4/videos/wyId/offsets"]
    
    #-----------------SET properties----------------#
    for key, value in zip(parameter_key, parameter_value):
      set_parameter('Wyscout_function', key, value)
    
    log_finished_process(logger, "Finished initialization of properties")

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function set_property                                   #
# Return nothing                                          #
# Set one properties in the properties table              #
###########################################################
def set_property(logger, key, value):
  try:
    log_starting_process(logger, "Starting to set one properties")

    set_parameter('Wyscout_function', key, value)

    log_finished_process(logger, "Finished to set one properties")

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_property                                   #
# Return nothing                                          #
# Get one properties from the properties table            #
###########################################################
def get_property(logger, key):
  try:
    log_starting_process(logger, "Starting to get one properties")

    value = get_parameter('Wyscout_function', key)

    log_finished_process(logger, "Finished to get one properties")

    return value

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_properties                                 #
# Return nothing                                          #
# Get all the properties from the properties table        #
###########################################################
def get_properties(logger):
  try:
    log_starting_process(logger, "Starting to get all the properties")

    parameter_key = ["transfer_start_year",
    "file_path",
    "authorization",
    "search_api_url",
    "areas_url_api",
    "list_competitions_url_api",
    "competitions_url_api",
    "competitions_players_url_api",
    "competitions_url_api_matches",
    "competitions_url_api_seasons",
    "competitions_url_api_teams",
    "seasons_url_api",
    "seasons_players_url_api",
    "seasons_transfers",
    "seasons_url_api_assistmen",
    "seasons_url_api_career",
    "seasons_url_api_fixtures",
    "seasons_url_api_matches",
    "seasons_url_api_scorers",
    "seasons_url_api_standings",
    "seasons_url_api_teams",
    "matches_url_api",
    "matches_url_api_formations",
    "matches_url_api_events",
    "matches_url_api_advancedstats",
    "matches_url_api_playeradvancedstats",
    "matches_url_api_directions",
    "matches_url_api_fulltracking",
    "teams_url_api",
    "teams_advanced_stats_url_api",
    "teams_transfers_url_api",
    "teams_url_api_career",
    "teams_url_api_fixtures",
    "teams_url_api_matches",
    "teams_url_api_squad",
    "players_url_api",
    "players_advanced_stats_url_api",
    "players_url_api_career",
    "players_url_api_contractinfo",
    "players_url_api_fixtures",
    "players_url_api_matches",
    "players_url_api_transfers",
    "rounds_url_api",
    "referees_url_api",
    "coaches_url_api",
    "player_match_advanced_stats_url_api",
    "team_match_advanced_stats_url_api",
    "videos_url_api",
    "videos_url_api_qualities",
    "videos_url_api_offsets"]
    
    parameter_value = []
    
    #-----------------GET properties----------------#
    for key in parameter_key:
      value = get_parameter('Wyscout_function', key)
      parameter_value.append(value)

    log_finished_process(logger, "Finished to get all the properties")

    return parameter_key, parameter_value

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_set_properties                             #
# Return config_file that contains all properties         #                                  
# Get the today date  to historize data                   #
# Get all the properties and date properties in properties#
# Create a dictionnary with all the properties            #
###########################################################
def get_set_properties(logger):
  try:
    log_starting_process(logger, "Starting to get all the properties & get today date")
    
    #-----------Get date to historize data----------#
    today_date = get_today_date(logger)
    today_date_folder = today_date[0]
    today_date_filename = today_date[1]

    #-----------------Get properties----------------#
    properties = get_properties(logger)
    properties_key = properties[0]
    properties_value = properties[1]

    #---------------Set date properties-------------#
    properties_key.append("today_date")
    properties_value.append(today_date_folder)
    properties_key.append("today_date_filename")
    properties_value.append(today_date_filename)

    #-----------Create DICT with properties---------#
    config_file = dict.fromkeys(properties_key)

    #--------------SET OTHER properties-------------#
    for key, value in zip(properties_key, properties_value):
      config_file[key]=value

    log_finished_process(logger, "Finished to get all the properties & get today date")

    return config_file

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_today_date_folder
# Return the date for the folder and the filename
# Get the date of today to historize data 
###########################################################
def get_today_date(logger):
  try:
    log_starting_process(logger, "Starting to get today date")

    #-----------------GET TODAY DATE----------------#
    now = datetime.now(pytz.timezone("Europe/Brussels"))
    today_date_folder = now.strftime("%Y/%m/%d")
    today_date_folder = today_date_folder + "/"
    today_date_filename = now.strftime("%Y_%m_%d")

    log_finished_process(logger, "Finished to get today date")

    return today_date_folder, today_date_filename

  except Exception as e:
    log_error_process(logger, e) 
###########################################################

###########################################################
# Function get_time
# Return the datetime of today
###########################################################
def get_time(logger):
  try:
    time = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d %H:%M:%S')
    
    return time

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function convert_watermark_to_folder_and_filename
# Return the foldername and filename of the watermark date
###########################################################
def convert_watermark_to_folder_and_filename(logger, watermark_date):
  try:
    today_date_folder = watermark_date.strftime("%Y/%m/%d")
    today_date_folder = today_date_folder + "/"
    today_date_filename = watermark_date.strftime("%Y_%m_%d")

    return today_date_folder, today_date_filename

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function encode_authorization
# Return an encoded base64 string
# Encode the username and password for the authentication
###########################################################
def get_headers_authorization(config_file, logger):
  try:
    log_starting_process(logger, "Starting Authorization encoding")
    
    #-------------Enconding authorization-----------#
    authorization = config_file["authorization"]
    authorization_bytes = authorization.encode('ascii')
    base64_bytes = base64.b64encode(authorization_bytes)
    base64_authorization = base64_bytes.decode('ascii')
    headers = {"Authorization" : "Basic "+base64_authorization}
    
    log_finished_process(logger, "Finished Authorization encoding")
    
    return headers

  except Exception as e:
    log_critical_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#----------------------Function FILE----------------------#
###########################################################

###########################################################
# Function check_file_exist                               #
# Return True if file exist else return False             #
# Check if the file exist                                 #
###########################################################
def check_file_exist(file_name, logger):
  try:
    if "?" in file_name:
        file_name = file_name.replace("?", "=" )

    try: 
      file = dbutils.fs.ls(file_name)
      return True

    except: 
      return False

  except Exception as e:
    print(e)
    log_error_process(logger, e)
###########################################################

###########################################################
# Function save_file                                      #
# Return nothing                                          #
# Save the file at the specified location                 #
###########################################################
def save_file(file_path, response, logger):
  try:
    if "?" in file_path:
        file_path = file_path.replace("?", "=" )

    if(check_file_exist(file_path, logger)==True):
      old_file = get_json_file(file_path, logger)
      old_file_hash = hash(str(old_file))
      new_file_hash = hash(response.text)

      if(old_file_hash != new_file_hash):
        dbutils.fs.rm(file_path)
        dbutils.fs.put(file_path, response.text)

    else:
      dbutils.fs.put(file_path, response.text)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_json_file                                  #
# Return the specified file                               #
# Get the file and load the json into data                #
###########################################################
def get_json_file(file_name, logger):
  try:
    if "?" in file_name:
      file_name = file_name.replace("?", "=" )
    
    data = spark.read.text(file_name, wholetext=True)
    data = data.head()[0]
    data = json.loads(data) 
    return data

  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#-------------------Request API Function------------------#
###########################################################

###########################################################
# Function request_get
# Return the response of the GET request
# Make a GET request on an API
###########################################################
def request_get(url,headers, logger):
  try:
    #-----Get file from url-----#
    response = requests.request("GET", url, headers=headers)
    return response

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function print_status
# Return nothing
# Print the error returned from the API
###########################################################
def log_status(url, status_code, text_response, logger):
  try:
    if(status_code != 200):
        status = url + text_response
        log_warning_process(logger, status)

  except Exception as e:
    log_error_process(logger, e)
        
###########################################################

###########################################################
# Function get_upload_file
# Return the status code from the GET request
# Call a function to make the GET request
# Check the status of the request
# If the status is OK call a function to save the file
# If the status is not OK, call a function to print the error
# If there is an error of time out or too much request 
# -> Wait 1 second and retry the request until it's the same error
###########################################################
def get_upload_file(config_file, headers, url, logger):
  try:
    #-----API request (GET)-----#
    file_path = config_file["file_path"]
    response = request_get(url, headers, logger)

    if response.status_code != 200:
        while((response.status_code==429) or (response.status_code==504)):
            log_status(url, response.status_code, response.text, logger)
            time.sleep(1)
            response = request_get(url, headers, logger)
        
        if response.status_code == 200:
            upload_file(config_file, url, response, logger)
            
        return response.status_code

    else:
        #-----Upload JSON file-----#
        upload_file(config_file, url, response, logger)
        return response.status_code

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function upload_file
# Return nothing
# Get the foldername and filename of the requested data
# Save the requested data in JSON file
###########################################################
def upload_file(config_file, url, response, logger):
  try:
    file_path = config_file["file_path"]

    #----Get foldername-----#
    folder_name = get_foldername(url, logger)
    file_path = file_path + folder_name
    
    #----Get date to historize data-----#
    file_path = file_path + config_file["today_date"]
    
    #----Get filename-----#
    filename = get_filename(url, response, logger)
    filename_timestamp = filename.split(".")[0] +"_"+ config_file["today_date_filename"] + ".json"

    #-----Save file in folder-----#
    save_file(file_path+filename_timestamp, response, logger)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_foldername
# Return the folder path and name of the requested data
# Decompose the URL to get the folder path and name
###########################################################
def get_foldername(url, logger):
  try:
    #-----get foldername of the file-----#
    url_split = url.split("/")
    url_len = len(url_split)
    
    if '?' in url_split[4]:
        second_folder = url_split[4].split("?")[1]

        if("query" in second_folder):
          second_folder=second_folder.split("objType=")[1]
          second_folder=second_folder.split("&")[0]

        elif("area" in second_folder):
          second_folder="areas"

        else:
          second_folder=""

        url_split[4] = url_split[4].split("?")[0]
        url_split[4] = url_split[4] + "/" + second_folder

    folder = url_split[4]+"/"

    if(url_len == 5):
      return folder

    elif(url_len == 6):
        return folder + url_split[4]+"/"

    elif(url_len == 7):
      if '?' in url_split[6]:
        url_split[6] = url_split[6].split("?")[0]

      folder = folder + url_split[6]+"/" 
      return folder

    elif(url_len == 8):
        folder = folder + url_split[6]+"_" + url_split[7]+"/"
        return folder

    elif(url_len == 9):
        folder = folder + url_split[6]+"_" + url_split[8]+"/"
        return folder

    else:
        return "error/"

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_filename
# Return the file name of the requested data
# Decompose the URL to get the file name
###########################################################
def get_filename(url, response, logger):
  try:
    #-----get filetype of the file-----#
    file_type = response.headers.get("Content-Type")

    if(file_type=="application/json"):
        file_type=".json"

    elif(file_type=="application/xml; charset=utf-8"):
        file_type=".xml"

    else:
        file_type=".unknown"

    #-----get filename of the file-----#
    url_split = url.split("/")
    url_len = len(url_split)

    if(url_len == 5):
        filename=url_split[4]+file_type
        return filename

    elif(url_len == 6):
        if '?useSides=1' in url_split[5]:
            url_split[5] = url_split[5].split("?useSides=1")[0]
        filename=url_split[4]+"_"+url_split[5]+file_type
        return filename

    elif(url_len == 7):
        if '?useSides=1' in url_split[6]:
            url_split[6] = url_split[6].split("?useSides=1")[0]
        filename=url_split[4]+"_"+url_split[6]+"_"+url_split[5]+file_type
        return filename

    elif(url_len == 8):
        filename=url_split[4]+"_"+url_split[6]+"_"+url_split[7]+"_"+url_split[5]+file_type
        return filename

    elif(url_len == 9):
        filename=url_split[4]+"_"+url_split[6]+"_"+url_split[8]+"_"+url_split[5]+"_"+url_split[7]+file_type
        return filename

    else:
        return "error.json"
  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#-----------------SEARCH REQUEST FUNCTION-----------------#
###########################################################

###########################################################
# Function search_request
# Return nothing
# Download the JSON file of the searched request
# Need to specify the searched string (name of the player/team,...)
# Need to specify the object type (competition / team / player / referee)
# Can specify the gender type (men / women) or leave blank to search both
###########################################################
def search_request(config_file, headers, query_string, obj_type, gender_type, logger):
  try:
    log_starting_process(logger, "Starting Search request")

    url = config_file["search_api_url"]
    parameter_query = "query_string"
    parameter_object = "obj_type"
    parameter_gender = "gender_type"

    url_split = url.split(parameter_query)
    url_split2 = url_split[1].split(parameter_object)
    url_split3 = url_split2[1].split(parameter_gender)
    api_url = url_split[0] + query_string + url_split2[0] + obj_type + url_split3[0] + gender_type
    status = get_upload_file(config_file, headers, api_url, logger)
    
    log_finished_process(logger, "Finished Search request")
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#------------------AREAS REQUEST FUNCTION-----------------#
###########################################################

###########################################################
# Function areas_request
# Return nothing
# Download the JSON file that contains the list of all areas
###########################################################    
def areas_request(config_file, headers, logger):
  try:
    log_starting_process(logger, "Starting Areas request")
    
    update_time = get_time(logger)

    api_url = config_file["areas_url_api"]
    status = get_upload_file(config_file, headers, api_url, logger)
    
    log_finished_process(logger, "Finished Areas request")

    set_raw_watermark_value('wyscout','areas',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#--------------COMPETITIONS REQUEST FUNCTION--------------#
###########################################################

###########################################################
# Function competitions_areas_request
# Return nothing
# Download all JSON file concerning all competitions of each areas
# Request :
# - List of competition for each area
###########################################################
def competitions_areas_request(config_file, headers, list_areas, logger):
  try:
    log_starting_process(logger, "Starting Competitions areas request")

    update_time = get_time(logger)

    url = config_file["list_competitions_url_api"]

    for area_id in list_areas:
        api_url = url + area_id
        status = get_upload_file(config_file, headers, api_url, logger)
    
    log_finished_process(logger, "Finished Competitions areas request")

    set_raw_watermark_value('wyscout','competitions_areas',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function competitions_details_request
# Return nothing
# Download all JSON file concerning all competitions details
###########################################################
def competitions_details_request(config_file, headers, list_competitions, logger):
  try:
    log_starting_process(logger, "Starting Competitions details request")

    update_time = get_time(logger)

    url = config_file["competitions_url_api"]
    
    nb_requests = len(list_competitions)
    for competition_id in list_competitions:
        api_url = url + competition_id
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Competitions details request")

    set_raw_watermark_value('wyscout','competitions_details',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function competitions_players_request
# Return nothing
# Download all JSON file concerning all competitions players
###########################################################
def competitions_players_request(config_file, headers, list_competitions, logger):
  try:
    log_starting_process(logger, "Starting Competitions players request")

    update_time = get_time(logger)

    url_players = config_file["competitions_players_url_api"]
    url_id_name = "wyId"
    
    nb_requests = len(list_competitions)
    for competition_id in list_competitions:
        current_page = 1
        url_split = url_players.split(url_id_name)
        api_url = url_split[0] + competition_id + url_split[1] + str(current_page)
        nb_page = get_nb_page_players(api_url,headers, logger)
        while(current_page <= nb_page):
            api_url = url_split[0] + competition_id + url_split[1] + str(current_page)
            status = get_upload_file(config_file, headers, api_url, logger)
            current_page+= 1

    log_finished_process(logger, "Finished Competitions players request")

    set_raw_watermark_value('wyscout','competitions_players',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function competitions_teams_request
# Return nothing
# Download all JSON file concerning all competitions teams
###########################################################
def competitions_teams_request(config_file, headers, list_competitions, logger):
  try:
    log_starting_process(logger, "Starting Competitions teams request")

    update_time = get_time(logger)

    url = config_file["competitions_url_api_teams"]
    url_id_name = "wyId"
    
    nb_requests = len(list_competitions)
    for competition_id in list_competitions:      
        url_split = url.split(url_id_name)
        api_url = url_split[0] + competition_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Competitions teams request")

    set_raw_watermark_value('wyscout','competitions_teams',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function competitions_matches_request
# Return nothing
# Download all JSON file concerning all competitions matches
###########################################################
def competitions_matches_request(config_file, headers, list_competitions, logger):
  try:
    log_starting_process(logger, "Starting Competitions matches request")

    update_time = get_time(logger)

    url = config_file["competitions_url_api_matches"]
    url_id_name = "wyId"
    
    nb_requests = len(list_competitions)
    for competition_id in list_competitions:      
        url_split = url.split(url_id_name)
        api_url = url_split[0] + competition_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Competitions matches request")

    set_raw_watermark_value('wyscout','competitions_matches',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function competitions_seasons_request
# Return nothing
# Download all JSON file concerning all competitions seasons
###########################################################
def competitions_seasons_request(config_file, headers, list_competitions, logger):
  try:
    log_starting_process(logger, "Starting Competitions seasons request")

    update_time = get_time(logger)

    url = config_file["competitions_url_api_seasons"]
    url_id_name = "wyId"
    
    nb_requests = len(list_competitions)
    for competition_id in list_competitions:      
        url_split = url.split(url_id_name)
        api_url = url_split[0] + competition_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Competitions seasons request")

    set_raw_watermark_value('wyscout','competitions_seasons',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#-----------------SEASONS REQUEST FUNCTION----------------#
###########################################################

###########################################################
# Function seasons_details_request
# Return nothing
# Download all JSON file concerning all seasons details of each competitions
###########################################################
def seasons_details_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons details request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api"]
    
    nb_requests = len(list_seasons)
    for season in list_seasons:
        api_url = url + season
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons details request")

    set_raw_watermark_value('wyscout','seasons_details',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_transfers_request
# Return nothing
# Download all JSON file concerning all seasons transfers of each competitions
###########################################################
def seasons_transfers_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons transfers request")

    update_time = get_time(logger)

    url_transfer = config_file["seasons_transfers"]
    url_id_name = "wyId"
    url_transfer_id_name = "date_from"
    url_transfer_id_name2 = "date_to"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:          
        date_from = get_date(config_file, "seasons", season, "startDate", logger)
        date_to = get_date(config_file, "seasons", season, "endDate", logger)
        url_transfer_split = url_transfer.split(url_id_name)
        url_transfer_split2 = url_transfer_split[1].split(url_transfer_id_name)
        url_transfer_split3 = url_transfer_split2[1].split(url_transfer_id_name2)
        api_url = url_transfer_split[0] + season + url_transfer_split2[0] + date_from + url_transfer_split3[0] + date_to
        status = get_upload_file(config_file, headers, api_url, logger)  

    log_finished_process(logger, "Finished Seasons transfers request")

    set_raw_watermark_value('wyscout','seasons_transfers',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_players_request
# Return nothing
# Download all JSON file concerning all seasons players of each competitions
###########################################################
def seasons_players_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons players request")

    update_time = get_time(logger)

    url_players = config_file["seasons_players_url_api"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:      
        current_page = 1
        url_split = url_players.split(url_id_name)
        api_url = url_split[0] + season + url_split[1] + str(current_page)
        nb_page = get_nb_page_players(api_url,headers, logger)

        while(current_page <= nb_page):
            api_url = url_split[0] + season + url_split[1] + str(current_page)
            status = get_upload_file(config_file, headers, api_url, logger)
            current_page+= 1

    log_finished_process(logger, "Finished Seasons players request")

    set_raw_watermark_value('wyscout','seasons_players',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_teams_request
# Return nothing
# Download all JSON file concerning all seasons teams of each competitions
###########################################################
def seasons_teams_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons teams request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api_teams"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:                  
        url_split = url.split(url_id_name)
        api_url = url_split[0] + season + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons teams request")

    set_raw_watermark_value('wyscout','seasons_teams',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_assistmen_request
# Return nothing
# Download all JSON file concerning all seasons assistmen of each competitions
###########################################################
def seasons_assistmen_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons assistmen request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api_assistmen"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:                  
        url_split = url.split(url_id_name)
        api_url = url_split[0] + season + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons assistmen request")

    set_raw_watermark_value('wyscout','seasons_assistmen',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_career_request
# Return nothing
# Download all JSON file concerning all seasons career of each competitions
###########################################################
def seasons_career_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons career request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api_career"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:                  
        url_split = url.split(url_id_name)
        api_url = url_split[0] + season + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons career request")

    set_raw_watermark_value('wyscout','seasons_careers',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_fixtures_request
# Return nothing
# Download all JSON file concerning all seasons fixtures of each competitions
###########################################################
def seasons_fixtures_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons fixtures request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api_fixtures"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:                  
        url_split = url.split(url_id_name)
        api_url = url_split[0] + season + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons fixtures request")

    set_raw_watermark_value('wyscout','seasons_fixtures',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_matches_request
# Return nothing
# Download all JSON file concerning all seasons matches of each competitions
###########################################################
def seasons_matches_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons matches request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api_matches"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:                  
        url_split = url.split(url_id_name)
        api_url = url_split[0] + season + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons matches request")

    set_raw_watermark_value('wyscout','seasons_matches',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_scorers_request
# Return nothing
# Download all JSON file concerning all seasons scorers of each competitions
###########################################################
def seasons_scorers_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons scorers request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api_scorers"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:                  
        url_split = url.split(url_id_name)
        api_url = url_split[0] + season + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons scorers request")

    set_raw_watermark_value('wyscout','seasons_scorers',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function seasons_standings_request
# Return nothing
# Download all JSON file concerning all seasons standings of each competitions
###########################################################
def seasons_standings_request(config_file, headers, list_seasons, logger):
  try:
    log_starting_process(logger, "Starting Seasons standings request")

    update_time = get_time(logger)

    url = config_file["seasons_url_api_standings"]
    url_id_name = "wyId"
    
    nb_requests = len(list_seasons)
    for season in list_seasons:                  
        url_split = url.split(url_id_name)
        api_url = url_split[0] + season + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Seasons standings request")

    set_raw_watermark_value('wyscout','seasons_standings',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

# COMMAND ----------

###########################################################
#-----------------MATCHES REQUEST FUNCTION----------------#
###########################################################

###########################################################
# Function matches_details_request
# Return nothing
# Download all JSON file concerning all matches details
###########################################################
def matches_details_request(config_file, headers, list_matches, logger): 
  try:
    log_starting_process(logger, "Starting Matches details request")   

    update_time = get_time(logger)

    url = config_file["matches_url_api"]
    url_id_name = "wyId"
    
    nb_requests = len(list_matches)
    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Matches details request")
    set_raw_watermark_value('wyscout','matches_details',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function matches_formations_request
# Return nothing
# Download all JSON file concerning all matches formations
###########################################################
def matches_formations_request(config_file, headers, list_matches, logger): 
  try:
    log_starting_process(logger, "Starting Matches formations request")   

    update_time = get_time(logger)

    url = config_file["matches_url_api_formations"]
    url_id_name = "wyId"
    
    nb_requests = len(list_matches)
    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)
          
    log_finished_process(logger, "Finished Matches formations request")
    set_raw_watermark_value('wyscout','matches_formations',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function matches_events_request
# Return nothing
# Download all JSON file concerning all matches events
###########################################################
def matches_events_request(config_file, headers, list_matches, logger): 
  try:
    log_starting_process(logger, "Starting Matches events request")   

    update_time = get_time(logger)

    url = config_file["matches_url_api_events"]
    url_id_name = "wyId"
    
    nb_requests = len(list_matches)
    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)
          
    log_finished_process(logger, "Finished Matches events request")
    set_raw_watermark_value('wyscout','matches_events',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function matches_advanced_stats_request
# Return nothing
# Download all JSON file concerning all matches advanced stats
###########################################################
def matches_advanced_stats_request(config_file, headers, list_matches, logger): 
  try:
    log_starting_process(logger, "Starting Matches advanced stats request")   

    update_time = get_time(logger)

    url = config_file["matches_url_api_advancedstats"]
    url_id_name = "wyId"
    
    nb_requests = len(list_matches)
    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)
          
    log_finished_process(logger, "Finished Matches advanced stats request")
    set_raw_watermark_value('wyscout','matches_advanced_stats',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function matches_player_advanced_stats_request
# Return nothing
# Download all JSON file concerning all matches players advanced stats
###########################################################
def matches_player_advanced_stats_request(config_file, headers, list_matches, logger): 
  try:
    log_starting_process(logger, "Starting Matches player advanced stats request")   

    update_time = get_time(logger)

    url = config_file["matches_url_api_playeradvancedstats"]
    url_id_name = "wyId"
    
    nb_requests = len(list_matches)
    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)
          
    log_finished_process(logger, "Finished Matches player advanced stats request")
    set_raw_watermark_value('wyscout','matches_player_advanced_stats',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function matches_fulltracking_request
# Return nothing
# Download all JSON file concerning all matches fulltracking
###########################################################
def matches_fulltracking_request(config_file, headers, list_matches, logger): 
  try:
    log_starting_process(logger, "Starting Matches fulltracking request")   

    update_time = get_time(logger)

    url = config_file["matches_url_api_fulltracking"]
    url_id_name = "wyId"
    
    nb_requests = len(list_matches)
    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)
          
    log_finished_process(logger, "Finished Matches fulltracking request")
    set_raw_watermark_value('wyscout','matches_fulltracking',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function matches_directions_request
# Return nothing
# Download all JSON file concerning all matches directions
###########################################################
def matches_directions_request(config_file, headers, list_matches, logger): 
  try:
    log_starting_process(logger, "Starting Matches directions request")   

    update_time = get_time(logger)

    url = config_file["matches_url_api_directions"]
    url_id_name = "wyId"
    
    nb_requests = len(list_matches)
    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)
          
    log_finished_process(logger, "Finished Matches directions request")
    set_raw_watermark_value('wyscout','matches_directions',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#------------------TEAMS REQUEST FUNCTION-----------------#
###########################################################

###########################################################
# Function teams_details_request
# Return nothing
# Download all JSON file concerning all teams details
###########################################################
def teams_details_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams details request")

    update_time = get_time(logger)

    url = config_file["teams_url_api"]
    
    nb_requests = len(list_teams)
    for team_id in list_teams:
        api_url = url + team_id
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Teams details request")

    set_raw_watermark_value('wyscout','teams_details',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function teams_career_request
# Return nothing
# Download all JSON file concerning all teams career
###########################################################
def teams_career_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams career request")

    update_time = get_time(logger)

    url = config_file["teams_url_api_career"]
    url_id_name = "wyId"
    
    nb_requests = len(list_teams)
    for team_id in list_teams:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + team_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Teams career request")

    set_raw_watermark_value('wyscout','teams_career',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function teams_fixtures_request
# Return nothing
# Download all JSON file concerning all teams fixtures
###########################################################
def teams_fixtures_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams fixtures request")

    update_time = get_time(logger)

    url = config_file["teams_url_api_fixtures"]
    url_id_name = "wyId"
    
    nb_requests = len(list_teams)
    for team_id in list_teams:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + team_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Teams fixtures request")

    set_raw_watermark_value('wyscout','teams_fixtures',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function teams_matches_request
# Return nothing
# Download all JSON file concerning all teams matches
###########################################################
def teams_matches_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams matches request")

    update_time = get_time(logger)

    url = config_file["teams_url_api_matches"]
    url_id_name = "wyId"
    
    nb_requests = len(list_teams)
    for team_id in list_teams:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + team_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Teams matches request")

    set_raw_watermark_value('wyscout','teams_matches',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function teams_squad_request
# Return nothing
# Download all JSON file concerning all teams squad
###########################################################
def teams_squad_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams squad request")

    update_time = get_time(logger)

    url = config_file["teams_url_api_squad"]
    url_id_name = "wyId"
    
    nb_requests = len(list_teams)
    for team_id in list_teams:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + team_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Teams squad request")

    set_raw_watermark_value('wyscout','teams_squad',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function teams_transfers_request
# Return nothing
# Download all JSON file concerning all teams transfers
###########################################################
def teams_transfers_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams transfers request")

    update_time = get_time(logger)

    init_start_year = int(config_file["transfer_start_year"])
    url_transfer = config_file["teams_transfers_url_api"]
    url_id_name = "wyId"
    url_transfer_id_name = "date_from"
    url_transfer_id_name2 = "date_to"
    
    now = datetime.now(pytz.timezone("Europe/Brussels"))
    year = now.strftime("%Y")
    current_year = int(year)
    
    for team_id in list_teams:
        start_year = init_start_year
        
        while (start_year <= current_year):
            date_from = str(start_year) + "-01-01"
            date_to = str(start_year) + "-12-31"
            url_transfer_split = url_transfer.split(url_id_name)
            url_transfer_split2 = url_transfer_split[1].split(url_transfer_id_name)
            url_transfer_split3 = url_transfer_split2[1].split(url_transfer_id_name2)
            api_url = url_transfer_split[0] + team_id + url_transfer_split2[0] + date_from + url_transfer_split3[0] + date_to
            status = get_upload_file(config_file, headers, api_url, logger)  
            start_year+=1
    
    log_finished_process(logger, "Finished Teams transfers request")

    set_raw_watermark_value('wyscout','teams_transfers',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function teams_advanced_stats_request
# Return nothing
# Download all JSON file concerning all teams advanced stats
###########################################################
def teams_advanced_stats_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams advanced stats request")

    update_time = get_time(logger)

    url_advanced_stats = config_file["teams_advanced_stats_url_api"]
    url_id_name = "wyId"

    watermark_date = get_raw_watermark_value("wyscout","teams_matches")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]
    
    nb_requests = len(list_teams)
    for team_id in list_teams:

          list_competitions = get_competition_id_matches(config_file, "teams/", team_id, logger, last_update_filename, last_update_foldername)

          for competition_id in list_competitions:
              url_split = url_advanced_stats.split(url_id_name)
              api_url = url_split[0] + team_id + url_split[1] + competition_id
              status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Teams advanced stats request")

    set_raw_watermark_value('wyscout','teams_advanced_stats',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function teams_advanced_stats_match_request
# Return nothing
# Download all JSON file of all teams advanced stats for a match of all matches of all seasons
###########################################################
def teams_advanced_stats_match_request(config_file, headers, list_teams, logger):
  try:
    log_starting_process(logger, "Starting Teams advanced stats/Match request")

    update_time = get_time(logger)

    url_advanced_stats = config_file["team_match_advanced_stats_url_api"]
    url_id_name = "wyId"
    url_id_name2 = "matchWyId"

    watermark_date = get_raw_watermark_value("wyscout","teams_matches")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]
    
    nb_requests = len(list_teams)
    for team_id in list_teams:
      team_tmp = [team_id]
      list_matches = get_matches_id(config_file, "teams/", team_tmp, logger, last_update_filename, last_update_foldername)
      nb_requests2 = len(list_matches)
      for match_id in list_matches:
          url_split = url_advanced_stats.split(url_id_name)
          url_split2 = url_split[1].split(url_id_name2)
          api_url = url_split[0] + team_id + url_split2[0] + match_id + url_split2[1]
          status = get_upload_file(config_file, headers, api_url, logger)
  
    log_finished_process(logger, "Finished Teams advanced stats/Match request")

    set_raw_watermark_value('wyscout','teams_matches_advanced_stats',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#-----------------PLAYERS REQUEST FUNCTION----------------#
###########################################################

###########################################################
# Function players_details_request
# Return nothing
# Download all JSON file concerning all players details 
###########################################################                    
def players_details_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players details request")

    update_time = get_time(logger)

    url = config_file["players_url_api"]
    
    nb_requests = len(list_players)
    for player_id in list_players:
        api_url = url + player_id
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players details request")

    set_raw_watermark_value('wyscout','players_details',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function players_transfers_request
# Return nothing
# Download all JSON file concerning all players transfers 
###########################################################                    
def players_transfers_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players transfers request")

    update_time = get_time(logger)

    url = config_file["players_url_api_transfers"]
    url_id_name = "wyId"
    
    nb_requests = len(list_players)
    for player_id in list_players:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + player_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players transfers request")

    set_raw_watermark_value('wyscout','players_transfers',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function players_career_request
# Return nothing
# Download all JSON file concerning all players career
###########################################################                    
def players_career_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players career request")

    update_time = get_time(logger)

    url = config_file["players_url_api_career"]
    url_id_name = "wyId"
    
    nb_requests = len(list_players)
    for player_id in list_players:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + player_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players career request")

    set_raw_watermark_value('wyscout','players_career',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function players_matches_request
# Return nothing
# Download all JSON file concerning all players matches
###########################################################                    
def players_matches_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players matches request")

    update_time = get_time(logger)

    url = config_file["players_url_api_matches"]
    url_id_name = "wyId"
    
    nb_requests = len(list_players)
    for player_id in list_players:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + player_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players matches request")

    set_raw_watermark_value('wyscout','players_matches',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function players_fixtures_request
# Return nothing
# Download all JSON file concerning all players fixtures
###########################################################                    
def players_fixtures_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players fixtures request")

    update_time = get_time(logger)

    url = config_file["players_url_api_fixtures"]
    url_id_name = "wyId"
    
    nb_requests = len(list_players)
    for player_id in list_players:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + player_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players fixtures request")

    set_raw_watermark_value('wyscout','players_fixtures',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function players_contract_info_request
# Return nothing
# Download all JSON file concerning all players 
###########################################################                    
def players_contract_info_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players contract info request")

    update_time = get_time(logger)

    url = config_file["players_url_api_contractinfo"]
    url_id_name = "wyId"
    
    nb_requests = len(list_players)
    for player_id in list_players:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + player_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players contract info request")

    set_raw_watermark_value('wyscout','players_contract_info',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function players_advanced_stats_request
# Return nothing
# Download all JSON file concerning all players advanced stats
###########################################################                    
def players_advanced_stats_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players advanced stats request")

    update_time = get_time(logger)

    url_advanced_stats = config_file["players_advanced_stats_url_api"]
    url_id_name = "wyId"

    watermark_date = get_raw_watermark_value("wyscout","players_matches")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]
    
    nb_requests = len(list_players)
    for player_id in list_players:
        list_competitions = get_competition_id_matches(config_file, "players/", player_id, logger, last_update_filename, last_update_foldername)
        for competition_id in list_competitions:
            url_split = url_advanced_stats.split(url_id_name)
            api_url = url_split[0] + player_id + url_split[1] + competition_id
            status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players advanced stats request")

    set_raw_watermark_value('wyscout','players_advanced_stats',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function players_advanced_stats_match_request
# Return nothing
# Download all JSON file of all players advanced stats for a match of all matches of all seasons
###########################################################
def players_advanced_stats_match_request(config_file, headers, list_players, logger):
  try:
    log_starting_process(logger, "Starting Players advanced stats/Match request")

    update_time = get_time(logger)
    
    url_advanced_stats = config_file["player_match_advanced_stats_url_api"]
    url_id_name = "wyId"
    url_id_name2 = "matchWyId"

    watermark_date = get_raw_watermark_value("wyscout","players_matches")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]
    
    nb_requests = len(list_players)
    for player_id in list_players:
        player_tmp = [player_id]
        list_matches = get_matches_id(config_file, "players/", player_tmp, logger, last_update_filename, last_update_foldername)
        nb_requests2 = len(list_matches)
        for match_id in list_matches:
            url_split = url_advanced_stats.split(url_id_name)
            url_split2 = url_split[1].split(url_id_name2)
            api_url = url_split[0] + player_id + url_split2[0] + match_id + url_split2[1]
            status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Players advanced stats/Match request")

    set_raw_watermark_value('wyscout','players_matches_advanced_stats',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#------------------OTHER REQUEST FUNCTION-----------------#
###########################################################

###########################################################
# Function rounds_request
# Return nothing
# Download all JSON file of all rounds of all seasons
# Request :
# - Round details for each round
###########################################################
def rounds_request(config_file, headers, list_rounds, logger):
  try:
    log_starting_process(logger, "Starting Rounds request")

    update_time = get_time(logger)

    url = config_file["rounds_url_api"]
    url_id_name = "wyId"
    
    nb_requests = len(list_rounds)
    for round_id in list_rounds:
        api_url = url + round_id
        status = get_upload_file(config_file, headers, api_url, logger)
    
    log_finished_process(logger, "Finished Rounds request")
  
    set_raw_watermark_value('wyscout','rounds',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function coaches_request
# Return nothing
# Download all JSON file of all coaches for each teams of all seasons
# Request :
# - Coach details for each coach of all teams
###########################################################
def coaches_request(config_file, headers, list_coaches, logger): 
  try: 
    log_starting_process(logger, "Starting Coaches request")

    update_time = get_time(logger)

    url = config_file["coaches_url_api"]
    url_id_name = "wyId"
    
    nb_requests = len(list_coaches)
    for coach_id in list_coaches:
        api_url = url + coach_id
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Coaches request")

    set_raw_watermark_value('wyscout','coaches',update_time)   

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function referees_request
# Return nothing
# Download all JSON file of all referees for each match of all seasons
# Request :
# - Referee details for each referee of all matches
###########################################################
def referees_request(config_file, headers, list_referees, logger):
  try:
    log_starting_process(logger, "Starting Referees request")
    
    update_time = get_time(logger)

    url = config_file["referees_url_api"]
    url_id_name = "wyId"
    
    nb_requests = len(list_referees)
    for referee_id in list_referees:
        api_url = url + referee_id
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished Referees request")

    set_raw_watermark_value('wyscout','referees',update_time)

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function videos_details_request
# Return nothing
# Download all JSON file of all match videos details for each match of all seasons
###########################################################
def videos_details_request(config_file, headers, list_matches, logger):
  try:
    log_starting_process(logger, "Starting videos details request")

    update_time = get_time(logger)

    url = config_file["videos_url_api"]

    for match_id in list_matches:
        api_url = url + match_id
        status = get_upload_file(config_file, headers, api_url, logger)
    
    log_finished_process(logger, "Finished videos details request")

    set_raw_watermark_value('wyscout','videos_details',update_time) 
          
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function videos_qualities_request
# Return nothing
# Download all JSON file of all match videos qualities for each match of all seasons
###########################################################
def videos_qualities_request(config_file, headers, list_matches, logger):
  try:
    log_starting_process(logger, "Starting videos qualities request")

    update_time = get_time(logger)

    url = config_file["videos_url_api_qualities"]
    url_id_name = "wyId"

    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)

    log_finished_process(logger, "Finished videos qualities request")

    set_raw_watermark_value('wyscout','videos_qualities',update_time) 

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function videos_offsets_request
# Return nothing
# Download all JSON file of all match videos offsets for each match of all seasons
###########################################################
def videos_offsets_request(config_file, headers, list_matches, logger):
  try:
    log_starting_process(logger, "Starting videos offsets request")

    update_time = get_time(logger)

    url = config_file["videos_url_api_offsets"]
    url_id_name = "wyId"

    for match_id in list_matches:
        url_split = url.split(url_id_name)
        api_url = url_split[0] + match_id + url_split[1]
        status = get_upload_file(config_file, headers, api_url, logger)
    
    log_finished_process(logger, "Finished videos offsets request")

    set_raw_watermark_value('wyscout','videos_offsets',update_time) 

  except Exception as e:
    log_error_process(logger, e)
###########################################################

# COMMAND ----------

###########################################################
#-----------------Function REGULAR UPDATE-----------------#
###########################################################

###########################################################
# Function get_current_seasons_id
# Return a set of all current season ID
# Read JSON file and get ID value for current season of each competition
###########################################################
def get_current_seasons_id(config_file, list_competitions, logger):
  try:
    list_seasons=[]
    folder = "competitions/seasons/"
    field_name = "seasons"
    id_name = "seasonId"

    watermark_date = get_raw_watermark_value("wyscout","competitions_seasons")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for competition in list_competitions:
        filename = "competitions_seasons_"+competition+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            
            for season in json_file["seasons"]:
                if(season["season"]["active"] == True):
                    list_seasons.append(season["seasonId"])
                    break

    return list_seasons
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_current_matches_id
# Return a set of all old match ID
# Read JSON file and get ID value for each old match of the season
###########################################################
def get_all_current_matches_id(config_file, list_seasons, logger):
  try:
    list_matches=[]
    folder = "seasons/matches/"
    field_name = "matches"
    id_name = "matchId"

    watermark_date = get_raw_watermark_value("wyscout","seasons_matches")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for season in list_seasons:
        filename = "seasons_matches_"+season+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_matches.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_matches)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_matches_rounds
# Return a set of all round ID of each match
# Read JSON file and get ID value for each round of each match
###########################################################
def get_all_matches_rounds(config_file, list_matches, logger):
  try:
    list_rounds=[]
    folder = "matches/matches/"
    field_name = "roundId"

    watermark_date = get_raw_watermark_value("wyscout","matches_details")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for match_id in list_matches:
        filename = "matches_"+match_id+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_rounds.append(str(json_file["roundId"]))
    
    myset = set(list_rounds)
    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_matches_teams_id
# Return a set of all team ID of each match
# Read JSON file and get ID value for each team of each match
###########################################################
def get_all_matches_teams_id(config_file, list_matches, logger):
  try:
    list_teams=[]
    folder = "matches/matches/"
    field_name = "teamsData"

    watermark_date = get_raw_watermark_value("wyscout","matches_details")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]
    
    for match_id in list_matches:
        filename = "matches_"+match_id+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_teams.append(str(json_file["teamsData"]["home"]["teamId"]))
            list_teams.append(str(json_file["teamsData"]["away"]["teamId"]))

    myset = set(list_teams)
    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_matches_players_id
# Return a set of all player ID of each match
# Read JSON file and get ID value for each player of each match
###########################################################
def get_all_matches_players_id(config_file, list_matches, logger):
  try:
    list_players=[]
    folder = "matches/advancedstats_players/"
    field_name = "players"
    id_name = "playerId"

    watermark_date = get_raw_watermark_value("wyscout","matches_player_advanced_stats")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]
    
    for match_id in list_matches:
        filename = "matches_advancedstats_players_"+match_id+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_players.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_players)
    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_teams_players_id
# Return a set of all players ID
# Read JSON file and get ID value for each players of each teams
###########################################################
def get_all_teams_players_id(config_file, list_teams, logger):
  try:
    list_players=[]
    folder = "teams/squad/"
    field_name = "squad"
    id_name = "wyId"

    watermark_date = get_raw_watermark_value("wyscout","teams_squad")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for team in list_teams:
        filename = "teams_squad_"+team+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_players.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_players)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function seasons_transfers_mercato_request
# Return nothing
# Download all JSON file concerning current seasons transfers (mercato)
###########################################################
def seasons_transfers_mercato_request(config_file, list_seasons, headers, logger):
  try:
    log_starting_process(logger, "Starting Seasons transfers mercato request")

    update_time = get_time(logger)

    url_transfer = config_file["seasons_transfers"]
    url_id_name = "wyId"
    url_transfer_id_name = "date_from"
    url_transfer_id_name2 = "date_to"

    last_update_date = str(get_raw_watermark_value("wyscout","last_update_transfers"))
    today_date = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d')
    
    for season in list_seasons:          
        #date_from = get_date(config_file, "seasons", season, "startDate", logger)
        #date_to = get_date(config_file, "seasons", season, "endDate", logger)
        date_from = last_update_date 
        date_to = today_date

        url_transfer_split = url_transfer.split(url_id_name)
        url_transfer_split2 = url_transfer_split[1].split(url_transfer_id_name)
        url_transfer_split3 = url_transfer_split2[1].split(url_transfer_id_name2)
        api_url = url_transfer_split[0] + season + url_transfer_split2[0] + date_from + url_transfer_split3[0] + date_to
        status = get_upload_file(config_file, headers, api_url, logger)  

    log_finished_process(logger, "Finished Seasons transfers mercato request")

    set_raw_watermark_value('wyscout','seasons_mercato_transfers',update_time)

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function get_all_current_transfers_team_id
# Return nothing
# Read JSON file and get ID value for each team of each transfer
###########################################################
def get_all_current_transfers_team_id(config_file, list_seasons, logger):
  try:
    list_teams=[]
    watermark_date = get_raw_watermark_value("wyscout","seasons_mercato_transfers")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    last_update_date = str(get_raw_watermark_value("wyscout","last_update_transfers"))
    today_date = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d')

    folder = "seasons/transfers/"
    field_name = "transfer"
    id_name = "fromTeamId"
    id_name2 = "toTeamId"
    
    for season in list_seasons:          
        #date_from = get_date(config_file, "seasons", season, "startDate", logger)
        #date_to = get_date(config_file, "seasons", season, "endDate", logger)
        date_from = last_update_date 
        date_to = today_date

        filename = "seasons_transfers=fromDate="+date_from+";toDate="+date_to+"_"+season+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)
        
        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            if("transfer" in json_file):
                if(len(json_file["transfer"])>0):
                    list_teams.extend(get_all_id(json_file, field_name, id_name, logger))
                    list_teams.extend(get_all_id(json_file, field_name, id_name2, logger))
    
    myset = set(list_teams)
    return myset

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

###########################################################
# Function get_all_current_transfers_players_id
# Return nothing
# Read JSON file and get ID value for each players of each transfer
###########################################################
def get_all_current_transfers_players_id(config_file, list_seasons, logger):
  try:
    list_players=[]
    watermark_date = get_raw_watermark_value("wyscout","seasons_mercato_transfers")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    last_update_date = str(get_raw_watermark_value("wyscout","last_update_transfers"))
    today_date = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y-%m-%d')

    folder = "seasons/transfers/"
    field_name = "transfer"
    id_name = "playerId"
    
    for season in list_seasons:          
        #date_from = get_date(config_file, "seasons", season, "startDate", logger)
        #date_to = get_date(config_file, "seasons", season, "endDate", logger)
        date_from = last_update_date 
        date_to = today_date
        
        filename = "seasons_transfers=fromDate="+date_from+";toDate="+date_to+"_"+season+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)
        
        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            if("transfer" in json_file):
                if(len(json_file["transfer"])>0):
                    list_players.extend(get_all_id(json_file, field_name, id_name, logger))
    
    myset = set(list_players)
    return myset

  except Exception as e:
    log_error_process(logger, e)    
###########################################################

# COMMAND ----------

###########################################################
#---------------------SEARCH FUNCTION---------------------#
###########################################################

###########################################################
# Function get_all_id
# Return a set of all requested values 
# Search all values of an array for a speciefied field
###########################################################
def get_all_id(json_file, field_name, id_name, logger):
  try:
    numlist=[]
    if field_name in json_file:
        nb = len(json_file[field_name])
        i=0

        while(i<nb):
            num = json_file[field_name][i][id_name]
            strnum = str(num)
            numlist.append(strnum)
            i=i+1
            
    myset = set(numlist)

    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_id
# Return a set of all requested values 
# Search all values of an array for a speciefied field
###########################################################
def get_id(json_file, field_name, logger):
  try:
    num = json_file[field_name]
    strnum = str(num)

    return strnum

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_nb_page_players
# Return the number of page of players 
# Read the JSON response from the request
# Search the number of page in the JSON response
###########################################################
def get_nb_page_players(url,headers, logger):
  try:
    response = request_get(url,headers, logger)
    text = response.text
    json_file = json.loads(text)
    nb_page = json_file["meta"]["page_count"]
    return nb_page

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_areas_id
# Return a set of all Areas ID
# Read the JSON file and get alpha3code value for each area
# The JSON file is a list of all areas
###########################################################
def get_all_areas_id(config_file, logger):
  try:
    watermark_date = get_raw_watermark_value("wyscout","areas")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    folder = "areas/"
    filename = "areas_"+last_update_filename+".json"
    field_name = "areas"
    id_name = "alpha3code"
    file_path = config_file["file_path"] + folder + last_update_filename + filename
    file_exist = check_file_exist(file_path, logger)

    if(file_exist==True):
      json_file = get_json_file(file_path, logger)
      list_areas = get_all_id(json_file, field_name, id_name, logger)
    else:
      list_areas=[]

    myset = set(list_areas)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_competitions_id
# Return a set of all Competition ID
# Read a JSON file and get ID value for each competition of the area
# JSON file is a list of all competitions for an area
# There is a file for each area
###########################################################
def get_all_competitions_id(config_file, list_areas, logger):
  try:
    list_competitions=[]
    folder = "competitions/areas/"
    field_name = "competitions"
    id_name = "wyId"

    watermark_date = get_raw_watermark_value("wyscout","competitions_areas")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for area_id in list_areas:
        filename = "competitions=areaId="+area_id+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_competitions.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_competitions)
    return myset
    
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_seasons_id
# Return a set of all season ID
# Read JSON file and get ID value for each season of the competition
# JSON file is a list of all season for a competition
# There is a file for each competition
###########################################################
def get_all_seasons_id(config_file, list_competitions, logger):
  try:
    list_seasons=[]
    folder = "competitions/seasons/"
    field_name = "seasons"
    id_name = "seasonId"

    watermark_date = get_raw_watermark_value("wyscout","competitions_seasons")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for competition in list_competitions:
        filename = "competitions_seasons_"+competition+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_seasons.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_seasons)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_matches_id
# Return a set of all match ID
# Read JSON file and get ID value for each match of the season
# JSON file is a list of all match for a season
# There is a file for each season
###########################################################
def get_all_matches_id(config_file, list_seasons, logger):
  try:
    list_matches=[]
    folder = "seasons/matches/"
    field_name = "matches"
    id_name = "matchId"

    watermark_date = get_raw_watermark_value("wyscout","seasons_matches")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for season in list_seasons:
        filename = "seasons_matches_"+season+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_matches.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_matches)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_teams_id
# Return a set of all team ID
# Read JSON file and get ID value for each team of the season
# JSON file is a list of all team for a season
# There is a file for each season
###########################################################
def get_all_teams_id(config_file, list_seasons, logger):
  try:
    list_teams=[]
    folder = "seasons/teams/"
    field_name = "teams"
    id_name = "wyId"

    watermark_date = get_raw_watermark_value("wyscout","seasons_teams")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for season in list_seasons:
        filename = "seasons_teams_"+season+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_teams.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_teams)
    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_players_id
# Return a set of all player ID
# Read JSON file and get ID value for each player of the season
# JSON file is a list of all players for a season
# There is a file for each season
###########################################################
def get_all_players_id(config_file, list_seasons, logger):
  try:
    list_players=[]
    folder = "seasons/players/"
    field_name = "players"
    id_name = "wyId"

    watermark_date = get_raw_watermark_value("wyscout","seasons_players")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for season in list_seasons:
        current_page = 1
        nb_pages = 1

        while(current_page <= nb_pages):
            filename = "seasons_players=limit=100&page="+str(current_page)+"_"+season+"_"+last_update_filename+".json"
            file_path = config_file["file_path"] + folder + last_update_foldername + filename
            file_exist = check_file_exist(file_path, logger)

            if(file_exist==True):
                json_file = get_json_file(file_path, logger)
                nb_pages = json_file["meta"]["page_count"]
                list_players.extend(get_all_id(json_file, field_name, id_name, logger))

            current_page+=1

    myset = set(list_players)
    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_rounds_id
# Return a set of all round ID
# Read JSON file and get ID value for each round of the season
# JSON file is a list of all round for a season
# There is a file for each season
###########################################################
def get_all_rounds_id(config_file, list_seasons, logger):
  try:
    list_rounds=[]
    folder = "seasons/career/"
    field_name = "rounds"
    id_name = "roundId"

    watermark_date = get_raw_watermark_value("wyscout","seasons_careers")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for season in list_seasons:
        filename = "seasons_career_"+season+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_rounds.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_rounds)
    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_referees_id
# Return a set of all referee ID
# Read JSON file and get ID value for each referee of each math
# JSON file is a match details
###########################################################
def get_all_referees_id(config_file, list_matches, logger):
  try:
    list_referees=[]
    folder = "matches/matches/"
    field_name = "referees"
    id_name = "refereeId"

    watermark_date = get_raw_watermark_value("wyscout","matches_details")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for match_id in list_matches:
        filename = "matches_"+match_id +"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + "/" + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_referees.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_referees)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_all_coaches_id
# Return a set of all coach ID
# Read JSON file and get ID value for each coach of each teams
# JSON file is a details of the squad for a team
# There is a file for each team
###########################################################
def get_all_coaches_id(config_file, list_teams, logger):
  try:
    list_coaches=[]
    folder = "teams/squad/"
    field_name = "coach"
    id_name = "wyId"

    watermark_date = get_raw_watermark_value("wyscout","teams_squad")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    for team in list_teams:
        filename = "teams_squad_"+team+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + folder + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_coaches.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_coaches)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_matches_id
# Return a set of all match ID
# Read JSON file and get match ID value for each match
# JSON file is a match details
###########################################################
def get_matches_id(config_file, search_folder, list_id, logger, last_update_filename, last_update_foldername):
  try:
    list_matches=[]
    field_name = "matches"
    id_name = "matchId"

    for current_id in list_id:
        filename = search_folder[:-1]+"_matches_"+current_id+"_"+last_update_filename+".json"
        file_path = config_file["file_path"] + search_folder + "matches/" + last_update_foldername + filename
        file_exist = check_file_exist(file_path, logger)

        if(file_exist==True):
            json_file = get_json_file(file_path, logger)
            list_matches.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_matches)
    return myset

  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_competition_id_matches
# Return a set of all competitions ID of a player/team
# Read JSON file and get competition ID value for each match
# JSON file is a match details
###########################################################
def get_competition_id_matches(config_file, search_folder, current_id, logger, last_update_filename, last_update_foldername):
  try:
    list_competitions=[]
    folder_name = "matches/"
    field_name = "matches"
    id_name = "competitionId"

    filename = search_folder[:-1]+"_matches_"+current_id+"_"+last_update_filename+".json"
    file_path = config_file["file_path"] + search_folder + "matches/" + last_update_foldername + filename
    file_exist = check_file_exist(file_path, logger)

    if(file_exist==True):
        json_file = get_json_file(file_path, logger)
        list_competitions.extend(get_all_id(json_file, field_name, id_name, logger))

    myset = set(list_competitions)
    return myset
  
  except Exception as e:
    log_error_process(logger, e)
###########################################################

###########################################################
# Function get_date
# Return the start date of the season (type : string)
# Read the JSON file and get the start date of the season
###########################################################
def get_date(config_file, search_folder, id_season, id_name, logger):
  try:
    watermark_date = get_raw_watermark_value("wyscout",search_folder+"_details")
    last_update_date = convert_watermark_to_folder_and_filename(logger, watermark_date)
    last_update_foldername = last_update_date[0]
    last_update_filename = last_update_date[1]

    filename =  search_folder+"_"+id_season + "_"+last_update_filename+".json"
    file_path = config_file["file_path"] + search_folder  + "/" + search_folder + "/" + last_update_foldername + filename
    file_exist = check_file_exist(file_path, logger)
    date=""

    if(file_exist==True):
        json_file = get_json_file(file_path, logger)
        date = json_file[id_name]
        
    return date

  except Exception as e:
    log_error_process(logger, e)
###########################################################
