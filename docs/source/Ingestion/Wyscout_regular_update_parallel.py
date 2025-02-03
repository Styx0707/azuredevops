# Databricks notebook source
# MAGIC %run "../Ingestion/Wyscout_function"

# COMMAND ----------

dbutils.widgets.text("data_source_name","")

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
#------------------COMPETITIONS PLAYERS-------------------#
###########################################################
def wyscout_competitions_players(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of competitions players")

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
      competitions_players_request(config_file, headers, list_competitions, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of competitions details players")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-------------------COMPETITIONS TEAMS--------------------#
###########################################################
def wyscout_competitions_teams(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of competitions teams")

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
      competitions_teams_request(config_file, headers, list_competitions, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of competitions teams completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------COMPETITIONS MATCHES-------------------#
###########################################################
def wyscout_competitions_matches(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of competitions matches")

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
      competitions_matches_request(config_file, headers, list_competitions, logger)

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
#-----------------------OLD MATCHES-----------------------#
###########################################################
def wyscout_set_old_matches(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Setting parameter of old matches")

    try:
      #################################################
      #--------------Configuration file---------------#
      #################################################
      config_file = get_set_properties(logger)
      
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
      #---------------Get old matches ID--------------#
      #################################################
      old_matches = get_all_current_matches_id(config_file, list_seasons, logger)
      
      #################################################
      #---------------Set old matches ID--------------#
      #################################################
      old_matches_str=""
      
      if(len(old_matches)>0):
        for match_id in old_matches:
            old_matches_str = old_matches_str + match_id + ","
        
        old_matches_str = old_matches_str[:-1]
      
      set_property(logger, "list_old_matches", old_matches_str)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Setting parameter old matches finished")
    
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
#---------------------SEASONS PLAYERS---------------------#
###########################################################
def wyscout_seasons_players(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons players")

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
      seasons_players_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons players completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#----------------------SEASONS TEAMS----------------------#
###########################################################
def wyscout_seasons_teams(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons teams")

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
      #---------Get list of current seasons ID--------#
      #################################################
      list_competitions = get_all_competitions_id(config_file, list_areas, logger)
      
      #################################################
      #-----------Get list of all seasons ID----------#
      #################################################
      list_seasons = get_current_seasons_id(config_file, list_competitions, logger)

      #################################################
      #----------------Seasons request----------------#
      #################################################
      seasons_teams_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons teams completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------SEASONS TRANSFERS--------------------#
###########################################################
def wyscout_seasons_transfers(logger):
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
      seasons_transfers_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons transfers completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------SEASONS ASSISTMEN--------------------#
###########################################################
def wyscout_seasons_assistmen(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons assistmen")

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
      seasons_assistmen_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons assistmen completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------SEASONS CAREER----------------------#
###########################################################
def wyscout_seasons_career(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons career")

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
      seasons_career_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons career completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------SEASONS FIXTURES---------------------#
###########################################################
def wyscout_seasons_fixtures(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons fixtures")

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
      seasons_fixtures_request(config_file, headers, list_seasons, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons fixtures completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------SEASONS MATCHES---------------------#
###########################################################
def wyscout_seasons_matches(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons matches")

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
      seasons_matches_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons matches completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------SEASONS SCORERS---------------------#
###########################################################
def wyscout_seasons_scorers(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons scorers")

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
      seasons_scorers_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons scorers completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------SEASONS_STANDINGS-------------------#
###########################################################
def wyscout_seasons_standings(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of seasons standings ")

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
      seasons_standings_request(config_file, headers, list_seasons, logger)
      
      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of seasons standings completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-----------------------NEW MATCHES-----------------------#
###########################################################
def wyscout_set_new_matches(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Setting parameter of new matches")

    try:
      #################################################
      #--------------Configuration file---------------#
      #################################################
      config_file = get_set_properties(logger)
      
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
      #---------------Get new matches ID--------------#
      #################################################
      new_matches = get_all_current_matches_id(config_file, list_seasons, logger)
      
      new_matches_str=""
      
      #################################################
      #----------Get parameter old matches ID---------#
      #################################################
      parameter_old_matches = get_property(logger, "list_old_matches")
      
      if(len(parameter_old_matches)>0):
        old_matches = parameter_old_matches.split(",")

        #################################################
        #--------Calculate DELTA of matches list--------#
        #################################################
        if(len(new_matches)>0):
            set_matches = set(new_matches) - set(old_matches)
            list_new_matches = list(set_matches)

            #################################################
            #---------------Set new matches ID--------------#
            #################################################
            
            for match_id in list_new_matches:
                new_matches_str = new_matches_str + match_id + ","
            
            new_matches_str = new_matches_str[:-1]
      
      set_property(logger, "list_new_matches", new_matches_str)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Setting parameter new matches finished")
    
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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
      
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")

        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
      
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")

        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)

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
#---------------------PLAYERS MATCHES---------------------#
###########################################################
def wyscout_players_matches(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players matches")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
      
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")

        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)

        #################################################
        #----------------Players request----------------#
        #################################################
        players_matches_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players matches completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#----------------------PLAYERS CAREER---------------------#
###########################################################
def wyscout_players_career(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players career")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
      
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")

        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)

        #################################################
        #----------------Players request----------------#
        #################################################
        players_career_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players career finished")
    
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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
      
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")

        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)
        
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
#---------------------PLAYERS FIXTURES--------------------#
###########################################################
def wyscout_players_fixtures(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players fixtures")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
           
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)

        #################################################
        #----------------Players request----------------#
        #################################################
        players_fixtures_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players fixtures finished")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------PLAYERS ADVANCED STATS-----------------#
###########################################################
def wyscout_players_advanced_stats(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players advanced stats")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")

        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)

        #################################################
        #----------------Players request----------------#
        #################################################
        players_advanced_stats_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players advanced stats finished")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------PLAYERS MATCHES ADVANCED STATS-------------#
###########################################################
def wyscout_players_matches_advanced_stats(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of players matches advanced stats")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_players = get_all_matches_players_id(config_file, list_matches, logger)

        #################################################
        #----------------Players request----------------#
        #################################################
        players_advanced_stats_match_request(config_file, headers, list_players, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of players matches advanced stats finished")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------MATCHES DETAILS----------------------#
###########################################################
def wyscout_matches_details(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of matches details")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #----------------Matches request----------------#
        #################################################
        matches_details_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of matches details completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-------------------MATCHES FORMATIONS--------------------#
###########################################################
def wyscout_matches_formations(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of matches formations")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
           
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #----------------Matches request----------------#
        #################################################
        matches_formations_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of matches formations completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-------------------MATCHES DIRECTIONS--------------------#
###########################################################
def wyscout_matches_directions(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of matches directions")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
           
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #----------------Matches request----------------#
        #################################################
        matches_directions_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of matches directions completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------MATCHES FULLTRACKING-------------------#
###########################################################
def wyscout_matches_fulltracking(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of matches fulltracking")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
           
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #----------------Matches request----------------#
        #################################################
        matches_fulltracking_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of matches fulltracking completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------MATCHES EVENTS----------------------#
###########################################################
def wyscout_matches_events(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of matches events")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
           
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #----------------Matches request----------------#
        #################################################
        matches_events_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of matches events completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-----------------MATCHES ADVANCED STATS------------------#
###########################################################
def wyscout_matches_advanced_stats(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of matches advanced stats")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #----------------Matches request----------------#
        #################################################
        matches_advanced_stats_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of matches advanced stats completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------MATCHES PLAYER ADVANCED STATS--------------#
###########################################################
def wyscout_matches_player_advanced_stats(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of matches player advanced stats")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #----------------Matches request----------------#
        #################################################
        matches_player_advanced_stats_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of matches advanced stats completed")
    
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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)

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
#----------------------TEAMS CAREER-----------------------#
###########################################################
def wyscout_teams_career(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams career")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)

        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_career_request(config_file, headers, list_teams, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams career completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------TEAMS FIXTURES----------------------#
###########################################################
def wyscout_teams_fixtures(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams fixtures")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)

        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_fixtures_request(config_file, headers, list_teams, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams fixtures completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#----------------------TEAMS MATCHES----------------------#
###########################################################
def wyscout_teams_matches(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams matches")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)

        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_matches_request(config_file, headers, list_teams, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams matches completed")
    
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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)
        
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
#------------------TEAMS ADVANCED STATS-------------------#
###########################################################
def wyscout_teams_advanced_stats(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams advanced stats")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)

        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_advanced_stats_request(config_file, headers, list_teams, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams completed advanced stats")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------TEAMS MATCHES ADVANCED STATS---------------#
###########################################################
def wyscout_teams_matches_advanced_stats(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of teams matches advanced stats")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
           
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)

        #################################################
        #-----------------Teams request-----------------#
        #################################################
        teams_advanced_stats_match_request(config_file, headers, list_teams, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of teams matches avanced stats completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------VIDEOS DETAILS----------------------#
###########################################################
def wyscout_videos_details(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of videos details")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
            
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #-----------------videos request----------------#
        #################################################
        videos_details_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of videos details completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#--------------------VIDEOS QUALITIES---------------------#
###########################################################
def wyscout_videos_qualities(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of videos qualities")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
                 
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #-----------------videos request----------------#
        #################################################
        videos_qualities_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of videos qualities completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#---------------------VIDEOS OFFSETS----------------------#
###########################################################
def wyscout_videos_offsets(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of videos offsets")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
                  
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #-----------------videos request----------------#
        #################################################
        videos_offsets_request(config_file, headers, list_matches, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of videos offsets completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#-------------------------ROUNDS--------------------------#
###########################################################
def wyscout_rounds(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of rounds")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
                  
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_rounds = get_all_matches_rounds(config_file, list_matches, logger)

        #################################################
        #----------------rounds request-----------------#
        #################################################
        rounds_request(config_file, headers, list_rounds, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of rounds completed")
    
    except Exception as e:
      log_critical_error_process(logger, e)
     
###########################################################

# COMMAND ----------

###########################################################
#------------------------REFEREES-------------------------#
###########################################################
def wyscout_referees(logger):
    #################################################
    #-------------------Init logger-----------------#
    #################################################
    log_starting_process(logger, "Starting download of referees")

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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
                 
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################      
        list_referees = get_all_referees_id(config_file, list_matches, logger)

        #################################################
        #----------------referees request---------------#
        #################################################
        referees_request(config_file, headers, list_referees, logger)

      #################################################
      #-------------------Close logger----------------#
      #################################################
      log_finished_process(logger, "Download of referees completed")
    
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
      #----------Get parameter new matches ID---------#
      #################################################
      parameter_new_matches = get_property(logger, "list_new_matches")
                  
      if(len(parameter_new_matches)>0):
        list_matches = parameter_new_matches.split(",")
        
        #################################################
        #------------Get list of different ID-----------#
        #################################################
        list_teams = get_all_matches_teams_id(config_file, list_matches, logger)
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

data_source_name=dbutils.widgets.get("data_source_name")

time_now = datetime.now(pytz.timezone("Europe/Brussels")).strftime('%Y_%m_%d_%H_%M_%S')

logger = logger_init(get_current_notebook_name()+"_"+data_source_name+"_"+time_now)

try :
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

    if(data_source_name=='competitions_players'):
        wyscout_competitions_players(logger)

    if(data_source_name=='competitions_teams'):
        wyscout_competitions_teams(logger)

    if(data_source_name=='competitions_matches'):
        wyscout_competitions_matches(logger)

    if(data_source_name=='competitions_seasons'):
        wyscout_competitions_seasons(logger)  

    #################################################
    #--------------------LEVEL 4--------------------#
    ################################################# 
    if(data_source_name=='set_old_matches'):
        wyscout_set_old_matches(logger)
    
    #################################################
    #--------------------LEVEL 5--------------------#
    #################################################
    if(data_source_name=='seasons_details'):
        wyscout_seasons_details(logger)

    if(data_source_name=='seasons_players'):
        wyscout_seasons_players(logger)

    if(data_source_name=='seasons_teams'):
        wyscout_seasons_teams(logger)
        
    if(data_source_name=='seasons_transfers'):
        wyscout_seasons_transfers(logger)

    if(data_source_name=='seasons_assistmen'):
        wyscout_seasons_assistmen(logger)    

    if(data_source_name=='seasons_careers'):
        wyscout_seasons_career(logger)

    if(data_source_name=='seasons_fixtures'):
        wyscout_seasons_fixtures(logger)

    if(data_source_name=='seasons_matches'):
        wyscout_seasons_matches(logger)
        
    if(data_source_name=='seasons_scorers'):
        wyscout_seasons_scorers(logger)

    if(data_source_name=='seasons_standings'):
        wyscout_seasons_standings(logger) 
    
    #################################################
    #--------------------LEVEL 6--------------------#
    #################################################
    if(data_source_name=='set_new_matches'):
        wyscout_set_new_matches(logger)

    #################################################
    #--------------------LEVEL 7-------------------#
    #################################################
    if(data_source_name=='players_details'):
        wyscout_players_details(logger)

    if(data_source_name=='players_transfers'):
        wyscout_players_transfers(logger)

    if(data_source_name=='players_matches'):
        wyscout_players_matches(logger)

    if(data_source_name=='players_career'):
        wyscout_players_career(logger)

    if(data_source_name=='players_contract_info'):
        wyscout_players_contract_info(logger)

    if(data_source_name=='players_fixtures'):
        wyscout_players_fixtures(logger)

    if(data_source_name=='teams_details'):
        wyscout_teams_details(logger)

    if(data_source_name=='teams_transfers'):
        wyscout_teams_transfers(logger)

    if(data_source_name=='teams_career'):
        wyscout_teams_career(logger)

    if(data_source_name=='teams_fixtures'):
        wyscout_teams_fixtures(logger)

    if(data_source_name=='teams_matches'):
        wyscout_teams_matches(logger)

    if(data_source_name=='teams_squad'):
        wyscout_teams_squad(logger)

    if(data_source_name=='matches_details'):
        wyscout_matches_details(logger)

    if(data_source_name=='matches_formations'):
        wyscout_matches_formations(logger)
    
    if(data_source_name=='matches_directions'):
        wyscout_matches_directions(logger)
    
    if(data_source_name=='matches_fulltracking'):
        wyscout_matches_fulltracking(logger)
        
    if(data_source_name=='matches_events'):
        wyscout_matches_events(logger)
        
    if(data_source_name=='matches_advanced_stats'):
        wyscout_matches_advanced_stats(logger)

    if(data_source_name=='matches_player_advanced_stats'):
        wyscout_matches_player_advanced_stats(logger)

    if(data_source_name=='rounds'):
        wyscout_rounds(logger)

    if(data_source_name=='referees'):
        wyscout_referees(logger)

    if(data_source_name=='videos_details'):
        wyscout_videos_details(logger)
        
    if(data_source_name=='videos_offsets'):
        wyscout_videos_offsets(logger)

    if(data_source_name=='videos_qualities'):
        wyscout_videos_qualities(logger)

    #################################################
    #--------------------LEVEL 8--------------------#
    #################################################
    if(data_source_name=='players_advanced_stats'):
        wyscout_players_advanced_stats(logger)

    if(data_source_name=='teams_advanced_stats'):
        wyscout_teams_advanced_stats(logger)

    if(data_source_name=='players_matches_advanced_stats'):
        wyscout_players_matches_advanced_stats(logger)

    if(data_source_name=='teams_matches_advanced_stats'):
        wyscout_teams_matches_advanced_stats(logger)

    if(data_source_name=='coaches'):
        wyscout_coaches(logger)

except Exception as e:
    log_critical_error_process(logger, e)
    logger_close(logger)

logger_close(logger)

#################################################
