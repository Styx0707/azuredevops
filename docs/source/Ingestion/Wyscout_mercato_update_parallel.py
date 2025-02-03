# Databricks notebook source
# MAGIC %run "../Ingestion/Wyscout_function"

# COMMAND ----------

dbutils.widgets.text("data_source_name","")

# COMMAND ----------

###########################################################
#----------------------MERCATO INIT-----------------------#
###########################################################
def wyscout_init_mercato(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting mercato initiation")

    try:
      #################################################
      #--------------Configuration file---------------#
      #################################################
      last_update_date = get_raw_watermark_value("wyscout","last_update_transfers")

      max_date = (datetime.now(pytz.timezone("Europe/Brussels"))- timedelta(days=365)).strftime('%Y-%m-%d')

      if(str(last_update_date)<max_date):
        update_time = get_time(logger)
        set_raw_watermark_value('wyscout','last_update_transfers',update_time)

      log_finished_process(logger, "Properties mercato completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------------AREAS--------------------------#
###########################################################
def wyscout_areas(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of areas")

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
      #-----------------Areas request-----------------#
      #################################################
      areas_request(config_file, headers, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of areas completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-------------------COMPETITIONS AREA---------------------#
###########################################################
def wyscout_areas_competitions(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of areas competitions")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #-------------Competitions request--------------#
      #--Get list of all competitions for each areas--#
      #################################################
      competitions_areas_request(config_file, headers, list_areas, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of areas competitions completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------COMPETITIONS DETAILS-------------------#
###########################################################
def wyscout_competitions_details(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of competitions details")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)

      #################################################
      #-------------Competitions request--------------#
      #################################################
      competitions_details_request(config_file, headers, list_competitions, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of competitions details completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------COMPETITIONS SEASONS-------------------#
###########################################################
def wyscout_competitions_seasons(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of competitions seasons")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)

      #################################################
      #-------------Competitions request--------------#
      #################################################
      competitions_seasons_request(config_file, headers, list_competitions, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of competitions details seasons")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------SEASONS DETAILS---------------------#
###########################################################
def wyscout_seasons_details(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons details")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)
      
      #################################################
      #----------------Seasons request----------------#
      #################################################
      seasons_details_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons details completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#----------------SEASONS MERCATO TRANSFERS----------------#
###########################################################
def wyscout_seasons_mercato_transfers(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons transfers")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)

      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #----------------Seasons request----------------#
      #################################################
      seasons_transfers_mercato_request(config_file, list_seasons, headers, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons transfers completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------PLAYERS DETAILS---------------------#
###########################################################
def wyscout_players_details(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players details")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #------------Get list of different ID-----------#
      #################################################
      list_players = get_all_current_transfers_players_id(config_file, list_seasons, logger)

      if(len(list_players)>0):
        #################################################
        #----------------Players request----------------#
        #################################################
        players_details_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players details completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------PLAYERS TRANSFERS--------------------#
###########################################################
def wyscout_players_transfers(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players transfers")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #------------Get list of different ID-----------#
      #################################################
      list_players = get_all_current_transfers_players_id(config_file, list_seasons, logger)
      
      if(len(list_players)>0):
        #################################################
        #----------------Players request----------------#
        #################################################
        players_transfers_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players transfers completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------PLAYERS CONTRACT INFO------------------#
###########################################################
def wyscout_players_contract_info(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players contract info")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #------------Get list of different ID-----------#
      #################################################
      list_players = get_all_current_transfers_players_id(config_file, list_seasons, logger)
        
      if(len(list_players)>0):
        #################################################
        #----------------Players request----------------#
        #################################################
        players_contract_info_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players contract info finished")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#----------------------TEAMS DETAILS----------------------#
###########################################################
def wyscout_teams_details(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams details")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #------------Get list of different ID-----------#
      #################################################
      list_teams = get_all_current_transfers_team_id(config_file, list_seasons, logger)
    
      if(len(list_teams)>0):
        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_details_request(config_file, headers, list_teams, logger)
    
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams details completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------TEAMS TRANSFERS----------------------#
###########################################################
def wyscout_teams_transfers(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams transfers")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #------------Get list of different ID-----------#
      #################################################
      list_teams = get_all_current_transfers_team_id(config_file, list_seasons, logger)
      
      if(len(list_teams)>0):
        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_transfers_request(config_file, headers, list_teams, logger)
    
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams transfers completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-----------------------TEAMS SQUAD-----------------------#
###########################################################
def wyscout_teams_squad(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams squad")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #------------Get list of different ID-----------#
      #################################################
      list_teams = get_all_current_transfers_team_id(config_file, list_seasons, logger)

      if(len(list_teams)>0):
        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_squad_request(config_file, headers, list_teams, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams squad completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------------COACHES--------------------------#
###########################################################
def wyscout_coaches(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of coaches")

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
      #-------Get list of all areas alpha3code--------#
      #################################################
      list_areas = get_all_areas_id(config_file, logger)
      list_areas = ["BEL"]

      #################################################
      #--------Get list of all competitions ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #---------Get list of current seasons ID--------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #------------Get list of different ID-----------#
      #################################################
      list_teams = get_all_current_transfers_team_id(config_file, list_seasons, logger)
      if(len(list_teams)>0):
        list_coaches = get_all_coaches_id(config_file, list_teams, logger)

        #################################################
        #----------------coaches request----------------#
        #################################################
        coaches_request(config_file, headers, list_coaches, logger) 

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of coaches completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#----------------SET DATE TRANSFERS UPDATE----------------#
###########################################################
def set_date_last_transfers_update(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Setting parameter of last update transfers")

    try:      
      #################################################
      #------------SET DATE TRANSFERS UPDATE----------#
      #################################################
      update_time = get_time(logger)
      set_raw_watermark_value('wyscout','last_update_transfers',update_time)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Setting parameter of last update transfers finished")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

data_source_name=dbutils.widgets.get("data_source_name")

time_now = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y_%m_%d_%H_%M_%S')

logger = logger_init(get_current_notebook_name()+"_"+data_source_name+"_"+time_now)

try :
    #################################################
    #--------------------LEVEL 0--------------------#
    #################################################
    if(data_source_name=='init_mercato'):
        wyscout_init_mercato(logger)

    #################################################
    #--------------------LEVEL 1--------------------#
    #################################################
    if(data_source_name=='areas'):
        wyscout_areas(logger)

    #################################################
    #--------------------LEVEL 2--------------------#
    #################################################
    if(data_source_name=='areas_competitions'):
        wyscout_areas_competitions(logger)
        
    #################################################
    #--------------------LEVEL 3--------------------#
    ################################################# 
    if(data_source_name=='competitions_details'):
        wyscout_competitions_details(logger)
        
    if(data_source_name=='competitions_seasons'):
        wyscout_competitions_seasons(logger)  
    
    #################################################
    #--------------------LEVEL 4--------------------#
    #################################################
    if(data_source_name=='seasons_details'):
        wyscout_seasons_details(logger)
        
    if(data_source_name=='seasons_mercato_transfers'):
        wyscout_seasons_mercato_transfers(logger)

    #################################################
    #--------------------LEVEL 5-------------------#
    #################################################
    if(data_source_name=='players_details'):
        wyscout_players_details(logger)

    if(data_source_name=='players_transfers'):
        wyscout_players_transfers(logger)

    if(data_source_name=='players_contract_info'):
        wyscout_players_contract_info(logger)

    if(data_source_name=='teams_details'):
        wyscout_teams_details(logger)

    if(data_source_name=='teams_transfers'):
        wyscout_teams_transfers(logger)

    if(data_source_name=='teams_squad'):
        wyscout_teams_squad(logger)

    #################################################
    #--------------------LEVEL 6--------------------#
    #################################################
    if(data_source_name=='coaches'):
        wyscout_coaches(logger)

    #################################################
    #--------------------LEVEL 7--------------------#
    #################################################
    if(data_source_name=='set_date_last_transfers_update'):
        set_date_last_transfers_update(logger)

except Exception as e:
    log_critical_error_process(logger, e)
    logger_close(logger)

logger_close(logger)

#################################################
