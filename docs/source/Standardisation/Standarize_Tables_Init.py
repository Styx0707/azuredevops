# Databricks notebook source
# MAGIC %run "../Framework/ETL Framework" 

# COMMAND ----------

# MAGIC %run "../Framework/Logging Management"

# COMMAND ----------

BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
STANDARIZED_RAW_CONTAINER=get_parameter("GLOBAL","STANDARIZED_RAW_CONTAINER")

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_areas tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_areas',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_areas'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(areas_alpha2code String, 
areas_alpha3code String, 
areas_id String,
areas_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_areas',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_coaches tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_coaches',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_coaches'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(birthDate String, 
currentNationalTeamId String, 
currentTeamId String,
firstName String ,
gender String,
imageDataURL String,
lastName String,
middleName String,
shortName String,
status String,
type String,
wyId String,
birthArea_alpha2code String,
birthArea_alpha3code String,
birthArea_id String,
birthArea_name String,
passportArea_alpha2code String,
passportArea_alpha3code String,
passportArea_id String,
passportArea_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_coaches',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_referees tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_referees',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_referees'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(birthDate String, 
firstName String ,
gender String, 
lastName String,
middleName String,
shortName String,
status String,
wyId String,
birthArea_alpha2code String,
birthArea_alpha3code String,
birthArea_id String,
birthArea_name String,
passportArea_alpha2code String,
passportArea_alpha3code String,
passportArea_id String,
passportArea_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_referees',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_rounds tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_rounds',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_rounds'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String, 
endDate String ,
name String, 
seasonId String,
startDate String,
type String,
wyId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_rounds',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_competitions_areas tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_competitions_areas',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_competitions_areas'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitions_category String, 
competitions_divisionLevel String ,
competitions_format String, 
competitions_gender String,
competitions_name String,
competitions_type String,
competitions_wyId String,
competitions_area_alpha2code String,
competitions_area_alpha3code String,
competitions_area_id String,
competitions_area_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_competitions_areas',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_competitions_matches tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_competitions_matches',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_competitions_matches'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matches_competitionId String, 
matches_date String ,
matches_dateutc String, 
matches_gameweek String,
matches_label String,
matches_matchId String,
matches_roundId String,
matches_seasonId String,
matches_status String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_competitions_matches',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_competitions_teams tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_competitions_teams',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_competitions_teams'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(teams_category String, 
teams_city String ,
teams_gender String, 
teams_imageDataURL String,
teams_name String,
teams_officialName String,
teams_type String,
teams_wyId String,
teams_area_alpha2code String,
teams_area_alpha3code String,
teams_area_id String,
teams_area_name String,
teams_children_name String,
teams_children_wyId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_competitions_teams',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_competitions_players tables       ################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_competitions_players',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_competitions_players'))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(meta_page_count String, 
meta_page_current String ,
meta_page_size String, 
meta_total_items String,
players_birthDate String,
players_currentNationalTeamId String,
players_currentTeamId String,
players_firstName String,
players_foot String,
players_gender String,
players_height String,
players_imageDataURL String,
players_lastName String,
players_middleName String,
players_shortName String,
players_status String,
players_weight String,
players_wyId String,
meta_appliedFilters_advsearch String,
meta_appliedFilters_fetch String,
meta_appliedFilters_filter String,
meta_appliedFilters_limit String,
meta_appliedFilters_page String,
meta_appliedFilters_search String,
meta_appliedFilters_sort String,
meta_availableFilters_advsearches String,
meta_availableFilters_fetches String,
meta_availableFilters_filters String,
meta_availableFilters_searches String,
meta_availableFilters_sorts String,
players_birthArea_alpha2code String,
players_birthArea_alpha3code String,
players_birthArea_id String,
players_birthArea_name String,
players_passportArea_alpha2code String,
players_passportArea_alpha3code String,
players_passportArea_id String,
players_passportArea_name String,
players_role_code2 String,
players_role_code3 String,
players_role_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)

LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_competitions_players',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_competitions_seasons tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_competitions_seasons',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_competitions_seasons'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String, 
seasons_seasonId String ,
seasons_season_active String, 
seasons_season_competitionId String,
seasons_season_endDate String,
seasons_season_name String,
seasons_season_startDate String,
seasons_season_wyId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_competitions_seasons',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_competitions tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_competitions_details',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_competitions_details'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(category String, 
divisionLevel String ,
format String, 
gender String,
name String,
type String,
wyId String,
area_alpha2code String,
area_alpha3code String,
area_id String,
area_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_competitions_details',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_details',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_details'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(category String, 
city String ,
gender String,
imageDataURL String,
name String,
officialName String,
type String,
wyId String,
area_alpha2code String,
area_alpha3code String,
area_id String,
area_name String,
children_name String,
children_wyId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_details',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_squad tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_squad',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_squad'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(coach_birthDate String ,
coach_currentNationalTeamId String,
coach_currentTeamId String,
coach_firstName String,
coach_gender String,
coach_imageDataURL String,
coach_lastName String,
coach_middleName String,
coach_shortName String,
coach_status String,
coach_type String,
coach_wyId String,
squad_birthDate String,
squad_currentNationalTeamId String,
squad_currentTeamId String,
squad_firstName String,
squad_foot String,
squad_gender String,
squad_height String,
squad_imageDataURL String,
squad_lastName String,
squad_middleName String,
squad_shortName String,
squad_status String,
squad_weight String,
squad_wyId String,
staff_birthDate String,
staff_currentNationalTeamId String,
staff_currentTeamId String,
staff_firstName String,
staff_gender String,
staff_imageDataURL String,
staff_lastName String,
staff_middleName String,
staff_shortName String,
staff_status String,
staff_type String,
staff_wyId String,
coach_birthArea_alpha2code String,
coach_birthArea_alpha3code String,
coach_birthArea_id String,
coach_birthArea_name String,
coach_passportArea_alpha2code String,
coach_passportArea_alpha3code String,
coach_passportArea_id String,
coach_passportArea_name String,
squad_birthArea_alpha2code String,
squad_birthArea_alpha3code String,
squad_birthArea_id String,
squad_birthArea_name String,
squad_passportArea_alpha2code String,
squad_passportArea_alpha3code String,
squad_passportArea_id String,
squad_passportArea_name String,
squad_role_code2 String,
squad_role_code3 String,
squad_role_name String,
staff_birthArea_alpha2code String,
staff_birthArea_alpha3code String,
staff_birthArea_id String,
staff_birthArea_name String,
staff_passportArea_alpha2code String,
staff_passportArea_alpha3code String,
staff_passportArea_id String,
staff_passportArea_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_squad',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_squad_temp tables        ##############################################
###########################################################################################
###########################################################################################

'''dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_squad',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_squad'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(staff String,
coach_birthDate String ,
coach_currentNationalTeamId String,
coach_currentTeamId String,
coach_firstName String,
coach_gender String,
coach_imageDataURL String,
coach_lastName String,
coach_middleName String,
coach_shortName String,
coach_status String,
coach_type String,
coach_wyId String,
squad_birthDate String,
squad_currentNationalTeamId String,
squad_currentTeamId String,
squad_firstName String,
squad_foot String,
squad_gender String,
squad_height String,
squad_imageDataURL String,
squad_lastName String,
squad_middleName String,
squad_shortName String,
squad_status String,
squad_weight String,
squad_wyId String,
coach_birthArea_alpha2code String,
coach_birthArea_alpha3code String,
coach_birthArea_id String,
coach_birthArea_name String,
coach_passportArea_alpha2code String,
coach_passportArea_alpha3code String,
coach_passportArea_id String,
coach_passportArea_name String,
squad_birthArea_alpha2code String,
squad_birthArea_alpha3code String,
squad_birthArea_id String,
squad_birthArea_name String,
squad_passportArea_alpha2code String,
squad_passportArea_alpha3code String,
squad_passportArea_id String,
squad_passportArea_name String,
squad_role_code2 String,
squad_role_code3 String,
squad_role_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_squad',STANDARIZED_RAW_CONTAINER))'''

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_career tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_career',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_career'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(career_competitionId String, 
career_goalAgainst String ,
career_goalPro String,
career_groupId String,
career_groupName String,
career_matchDraw String,
career_matchLost String,
career_matchTotal String,
career_matchWon String,
career_points String,
career_rank String,
career_roundId String,
career_roundName String,
career_seasonId String,
career_teamId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_career',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_fixtures        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_fixtures',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_fixtures'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matches_competitionId String,
matches_date String,
matches_dateutc String,
matches_gameweek String,
matches_label String,
matches_matchId String,
matches_roundId String,
matches_seasonId String,
matches_status String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_fixtures',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_matches tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_matches',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_matches'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matches_competitionId String, 
matches_date String ,
matches_dateutc String,
matches_gameweek String,
matches_label String,
matches_matchId String,
matches_roundId String,
matches_seasonId String,
matches_status String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_matches',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_transfers tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_transfers',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_transfers'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(transfer_active String,
transfer_announceDate String,
transfer_currency String,
transfer_endDate String,
transfer_fromTeamId String,
transfer_fromTeamName String,
transfer_playerId String,
transfer_startDate String,
transfer_toTeamId String,
transfer_toTeamName String,
transfer_type string,
transfer_value String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_transfers',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_advanced_stats tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_advanced_stats',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_advanced_stats'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String,
roundId String,
seasonId String,
teamId String,
average_accelerations String,
average_aerialDuels String,
average_assists String,
average_attackingActions String,
average_backPasses String,
average_ballLosses String,
average_ballRecoveries String,
average_concededGoals String,
average_corners String,
average_crosses String,
average_dangerousOpponentHalfRecoveries String,
average_dangerousOwnHalfLosses String,
average_defensiveActions String,
average_defensiveDuels String,
average_defensiveDuelsWon String,
average_directFreeKicks String,
average_directFreeKicksOnTarget String,
average_directRedCards String,
average_dribbleDistanceFromOpponentGoal String,
average_dribbles String,
average_duels String,
average_duelsWon String,
average_fieldAerialDuels String,
average_fieldAerialDuelsWon String,
average_forwardPasses String,
average_fouls String,
average_freeKicks String,
average_freeKicksOnTarget String,
average_gkAerialDuels String,
average_gkAerialDuelsWon String,
average_gkExits String,
average_gkGoalKicks String,
average_gkGoalKicksSuccess String,
average_gkSaves String,
average_gkSuccessfulExits String,
average_goals String,
average_headShots String,
average_interceptions String,
average_keyPasses String,
average_lateralPasses String,
average_linkupPlays String,
average_longGoalKicks String,
average_longPassLength String,
average_longPasses String,
average_looseBallDuels String,
average_looseBallDuelsWon String,
average_missedBalls String,
average_newDefensiveDuelsWon String,
average_newDuelsWon String,
average_newOffensiveDuelsWon String,
average_newSuccessfulDribbles String,
average_offensiveDuels String,
average_offensiveDuelsWon String,
average_offsides String,
average_opponentHalfRecoveries String,
average_opponentOffsides String,
average_ownHalfLosses String,
average_passLength String,
average_passes String,
average_passesToFinalThird String,
average_penalties String,
average_possessionPercent String,
average_progressiveRun String,
average_receivedPass String,
average_redCards String,
average_shortGoalKicks String,
average_shotAssists String,
average_shotOnTargetAssists String,
average_shots String,
average_shotsAgainst String,
average_smartPasses String,
average_successfulAttackingActions String,
average_successfulBackPasses String,
average_successfulCrosses String,
average_successfulDefensiveAction String,
average_successfulDribbles String,
average_successfulForwardPasses String,
average_successfulKeyPasses String,
average_successfulLateralPasses String,
average_successfulLinkupPlays String,
average_successfulLongPasses String,
average_successfulPasses String,
average_successfulPassesToFinalThird String,
average_successfulPenalties String,
average_successfulSmartPasses String,
average_successfulThroughPasses String,
average_successfulVerticalPasses String,
average_throughPasses String,
average_touchInBox String,
average_verticalPasses String,
average_xgShot String,
average_xgShotAgainst String,
average_yellowCards String,
percent_aerialDuelsWon String,
percent_defensiveDuelsWon String,
percent_directFreeKicksOnTarget String,
percent_duelsWon String,
percent_fieldAerialDuelsWon String,
percent_gkAerialDuelsWon String,
percent_gkSaves String,
percent_gkSuccessfulExits String,
percent_goalConversion String,
percent_headShotsOnTarget String,
percent_newDefensiveDuelsWon String,
percent_newDuelsWon String,
percent_newOffensiveDuelsWon String,
percent_newSuccessfulDribbles String,
percent_offensiveDuelsWon String,
percent_penaltiesConversion String,
percent_shotsOnTarget String,
percent_successfulBackPasses String,
percent_successfulCrosses String,
percent_successfulDribbles String,
percent_successfulForwardPasses String,
percent_successfulKeyPasses String,
percent_successfulLateralPasses String,
percent_successfulLinkupPlays String,
percent_successfulLongPasses String,
percent_successfulPasses String,
percent_successfulPassesToFinalThird String,
percent_successfulShotAssists String,
percent_successfulSmartPasses String,
percent_successfulThroughPasses String,
percent_successfulTouchInBox String,
percent_successfulVerticalPasses String,
percent_win String,
percent_yellowCardsPerFoul String,
total_accelerations String,
total_aerialDuels String,
total_aerialDuelsWon String,
total_assists String,
total_attackingActions String,
total_backPasses String,
total_cleanSheets String,
total_concededGoals String,
total_corners String,
total_crosses String,
total_dangerousOpponentHalfRecoveries String,
total_dangerousOwnHalfLosses String,
total_defensiveActions String,
total_defensiveDuels String,
total_defensiveDuelsWon String,
total_directFreeKicks String,
total_directFreeKicksOnTarget String,
total_directRedCards String,
total_dribbles String,
total_duels String,
total_duelsWon String,
total_fieldAerialDuels String,
total_fieldAerialDuelsWon String,
total_forwardPasses String,
total_fouls String,
total_freeKicks String,
total_freeKicksOnTarget String,
total_gkAerialDuels String,
total_gkAerialDuelsWon String,
total_gkExits String,
total_gkGoalKicks String,
total_gkGoalKicksSuccess String,
total_gkSaves String,
total_gkSuccessfulExits String,
total_goals String,
total_headShots String,
total_interceptions String,
total_keyPasses String,
total_lateralPasses String,
total_linkupPlays String,
total_longGoalKicks String,
total_longPasses String,
total_looseBallDuels String,
total_looseBallDuelsWon String,
total_losses String,
total_matches String,
total_matchesTagged String,
total_missedBalls String,
total_newDefensiveDuelsWon String,
total_newDuelsWon String,
total_newOffensiveDuelsWon String,
total_newSuccessfulDribbles String,
total_offensiveDuels String,
total_offensiveDuelsWon String,
total_offsides String,
total_opponentHalfRecoveries String,
total_opponentOffsides String,
total_ownHalfLosses String,
total_passes String,
total_passesToFinalThird String,
total_penalties String,
total_ppda String,
total_pressingDuels String,
total_pressingDuelsWon String,
total_progressiveRun String,
total_receivedPass String,
total_recoveries String,
total_redCards String,
total_shortGoalKicks String,
total_shotAssists String,
total_shotOnTargetAssists String,
total_shots String,
total_shotsAgainst String,
total_smartPasses String,
total_successfulAttackingActions String,
total_successfulBackPasses String,
total_successfulCrosses String,
total_successfulDefensiveActions String,
total_successfulDribbles String,
total_successfulForwardPasses String,
total_successfulKeyPasses String,
total_successfulLateralPasses String,
total_successfulLinkupPlays String,
total_successfulLongPasses String,
total_successfulPasses String,
total_successfulPassesToFinalThird String,
total_successfulPenalties String,
total_successfulSmartPasses String,
total_successfulThroughPasses String,
total_successfulVerticalPasses String,
total_throughPasses String,
total_touchInBox String,
total_verticalPasses String,
total_xgShot String,
total_xgShotAgainst String,
total_yellowCards String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_advanced_stats',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_teams_matches_advanced_stats tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_teams_matches_advanced_stats',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_teams_matches_advanced_stats'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String,
matchId String,
roundId String,
seasonId String,
teamId String,
average_accelerations String,
average_aerialDuels String,
average_assists string,
average_attackingActions String,
average_backPasses String,
average_ballLosses String,
average_ballRecoveries String,
average_concededGoals String,
average_corners String,
average_crosses String,
average_dangerousOpponentHalfRecoveries String,
average_dangerousOwnHalfLosses String,
average_defensiveActions String,
average_defensiveDuels String,
average_defensiveDuelsWon String,
average_directFreeKicks String,
average_directFreeKicksOnTarget String,
average_directRedCards String,
average_dribbleDistanceFromOpponentGoal String,
average_dribbles String,
average_duels String,
average_duelsWon String,
average_fieldAerialDuels String,
average_fieldAerialDuelsWon String,
average_forwardPasses String,
average_fouls String,
average_freeKicks String,
average_freeKicksOnTarget String,
average_gkAerialDuels String,
average_gkAerialDuelsWon String,
average_gkExits String,
average_gkGoalKicks String,
average_gkGoalKicksSuccess String,
average_gkSaves String,
average_gkSuccessfulExits String,
average_goals String,
average_headShots String,
average_interceptions String,
average_keyPasses String,
average_lateralPasses String,
average_linkupPlays String,
average_longGoalKicks String,
average_longPassLength String,
average_longPasses String,
average_looseBallDuels String,
average_looseBallDuelsWon String,
average_missedBalls String,
average_newDefensiveDuelsWon String,
average_newDuelsWon String,
average_newOffensiveDuelsWon String,
average_newSuccessfulDribbles String,
average_offensiveDuels String,
average_offensiveDuelsWon String,
average_offsides String,
average_opponentHalfRecoveries String,
average_opponentOffsides String,
average_ownHalfLosses String,
average_passLength String,
average_passes String,
average_passesToFinalThird String,
average_penalties String,
average_possessionPercent String,
average_progressiveRun String,
average_receivedPass String,
average_redCards String,
average_shortGoalKicks String,
average_shotAssists string,
average_shotOnTargetAssists String,
average_shots String,
average_shotsAgainst String,
average_smartPasses String,
average_successfulAttackingActions String,
average_successfulBackPasses String,
average_successfulCrosses String,
average_successfulDefensiveAction String,
average_successfulDribbles String,
average_successfulForwardPasses String,
average_successfulKeyPasses String,
average_successfulLateralPasses String,
average_successfulLinkupPlays String,
average_successfulLongPasses String,
average_successfulPasses String,
average_successfulPassesToFinalThird String,
average_successfulPenalties String,
average_successfulSmartPasses String,
average_successfulThroughPasses String,
average_successfulVerticalPasses String,
average_throughPasses String,
average_touchInBox String,
average_verticalPasses String,
average_xgShot String,
average_xgShotAgainst String,
average_yellowCards String,
percent_aerialDuelsWon String,
percent_defensiveDuelsWon String,
percent_directFreeKicksOnTarget String,
percent_duelsWon String,
percent_fieldAerialDuelsWon String,
percent_gkAerialDuelsWon String,
percent_gkSaves String,
percent_gkSuccessfulExits String,
percent_goalConversion String,
percent_headShotsOnTarget String,
percent_newDefensiveDuelsWon String,
percent_newDuelsWon String,
percent_newOffensiveDuelsWon String,
percent_newSuccessfulDribbles String,
percent_offensiveDuelsWon String,
percent_penaltiesConversion String,
percent_shotsOnTarget String,
percent_successfulBackPasses String,
percent_successfulCrosses String,
percent_successfulDribbles String,
percent_successfulForwardPasses String,
percent_successfulKeyPasses String,
percent_successfulLateralPasses String,
percent_successfulLinkupPlays String,
percent_successfulLongPasses String,
percent_successfulPasses String,
percent_successfulPassesToFinalThird String,
percent_successfulShotAssists String,
percent_successfulSmartPasses String,
percent_successfulThroughPasses String,
percent_successfulTouchInBox String,
percent_successfulVerticalPasses String,
percent_win String,
percent_yellowCardsPerFoul String,
total_accelerations String,
total_aerialDuels String,
total_aerialDuelsWon String,
total_assists String,
total_attackingActions String,
total_backPasses String,
total_cleanSheets String,
total_concededGoals String,
total_corners String,
total_crosses String,
total_dangerousOpponentHalfRecoveries String,
total_dangerousOwnHalfLosses String,
total_defensiveActions String,
total_defensiveDuels String,
total_defensiveDuelsWon String,
total_directFreeKicks String,
total_directFreeKicksOnTarget String,
total_directRedCards String,
total_dribbles String,
total_duels String,
total_duelsWon String,
total_fieldAerialDuels String,
total_fieldAerialDuelsWon String,
total_forwardPasses String,
total_fouls String,
total_freeKicks String,
total_freeKicksOnTarget String,
total_gkAerialDuels String,
total_gkAerialDuelsWon String,
total_gkExits String,
total_gkGoalKicks String,
total_gkGoalKicksSuccess String,
total_gkSaves String,
total_gkSuccessfulExits String,
total_goals String,
total_headShots String,
total_interceptions String,
total_keyPasses String,
total_lateralPasses String,
total_linkupPlays String,
total_longGoalKicks String,
total_longPasses String,
total_looseBallDuels String,
total_looseBallDuelsWon String,
total_losses String,
total_matches String,
total_matchesTagged String,
total_missedBalls String,
total_newDefensiveDuelsWon String,
total_newDuelsWon String,
total_newOffensiveDuelsWon String,
total_newSuccessfulDribbles String,
total_offensiveDuels String,
total_offensiveDuelsWon String,
total_offsides String,
total_opponentHalfRecoveries String,
total_opponentOffsides String,
total_ownHalfLosses String,
total_passes String,
total_passesToFinalThird String,
total_penalties String,
total_ppda String,
total_pressingDuels String,
total_pressingDuelsWon String,
total_progressiveRun String,
total_receivedPass String,
total_recoveries String,
total_redCards String,
total_shortGoalKicks String,
total_shotAssists String,
total_shotOnTargetAssists String,
total_shots String,
total_shotsAgainst String,
total_smartPasses string,
total_successfulAttackingActions String,
total_successfulBackPasses String,
total_successfulCrosses String,
total_successfulDefensiveActions String,
total_successfulDribbles String,
total_successfulForwardPasses String,
total_successfulKeyPasses String,
total_successfulLateralPasses String,
total_successfulLinkupPlays String,
total_successfulLongPasses String,
total_successfulPasses String,
total_successfulPassesToFinalThird String,
total_successfulPenalties String,
total_successfulSmartPasses String,
total_successfulThroughPasses String,
total_successfulVerticalPasses String,
total_throughPasses String,
total_touchInBox String,
total_verticalPasses String,
total_xgShot String,
total_xgShotAgainst String,
total_yellowCards String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_teams_matches_advanced_stats',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_details',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_details'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(active String, 
competitionId String ,
endDate String,
name String,
startDate String,
wyId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_details',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_assistmen tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_assistmen',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_assistmen'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String ,
seasonId String,
players_assists String,
players_playerId String,
players_teamId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_assistmen',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_careers tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_careers',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_careers'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(rounds_roundId String ,
rounds_groups_teams_competitionId String,
rounds_groups_teams_gameDraw String,
rounds_groups_teams_gameLost String,
rounds_groups_teams_gameTotal String,
rounds_groups_teams_gameWon String,
rounds_groups_teams_goalAgainst String,
rounds_groups_teams_goalPro String,
rounds_groups_teams_groupId String,
rounds_groups_teams_groupName String,
rounds_groups_teams_points String,
rounds_groups_teams_rank String,
rounds_groups_teams_roundId String,
rounds_groups_teams_roundName String,
rounds_groups_teams_seasonId String,
rounds_groups_teams_teamId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_careers',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_fixtures tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_fixtures',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_fixtures'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String ,
seasonId String,
matches_matchId String,
matches_goals_minute String,
matches_goals_period String,
matches_goals_playerId String,
matches_goals_teamId String,
matches_goals_type String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_fixtures',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_matches tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_matches',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_matches'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matches_competitionId String ,
matches_date String,
matches_dateutc String,
matches_gameweek String,
matches_label String,
matches_matchId String,
matches_roundId String,
matches_seasonId String,
matches_status String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_matches',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_players tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_players',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_players'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(meta_page_count String,
meta_page_current String,
meta_page_size String,
meta_total_items String,
players_birthDate String,
players_currentNationalTeamId String,
players_currentTeamId String,
players_firstName String,
players_foot String,
players_gender String,
players_height String,
players_imageDataURL String,
players_lastName String,
players_middleName String,
players_shortName String,
players_status String,
players_weight String,
players_wyId String,
meta_appliedFilters_advsearch String,
meta_appliedFilters_fetch String,
meta_appliedFilters_filter String,
meta_appliedFilters_limit String,
meta_appliedFilters_page String,
meta_appliedFilters_search String,
meta_appliedFilters_sort String,
meta_availableFilters_advsearches String,
meta_availableFilters_fetches String,
meta_availableFilters_filters String,
meta_availableFilters_searches String,
meta_availableFilters_sorts String,
players_birthArea_alpha2code String,
players_birthArea_alpha3code String,
players_birthArea_id String,
players_birthArea_name String,
players_passportArea_alpha2code String,
players_passportArea_alpha3code String,
players_passportArea_id String,
players_passportArea_name String,
players_role_code2 String,
players_role_code3 String,
players_role_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_players',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_scorers tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_scorers',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_scorers'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String ,
seasonId String,
players_goals String,
players_playerId String,
players_teamId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_scorers',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_teams tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_teams',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_teams'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(teams_category String ,
teams_city String,
teams_gender String,
teams_imageDataURL String,
teams_name String,
teams_officialName String,
teams_type String,
teams_wyId String,
teams_area_alpha2code String,
teams_area_alpha3code String,
teams_area_id String,
teams_area_name String,
teams_children_name String,
teams_children_wyId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_teams',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_standings tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_standings',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_standings'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String ,
seasonId String,
teams_groupName String,
teams_teamId String,
teams_totalDraws String,
teams_totalGoalsAgainst String,
teams_totalGoalsFor String,
teams_totalLosses String,
teams_totalPlayed String,
teams_totalPoints String,
teams_totalWins String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_standings',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_seasons_transfers tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_seasons_transfers',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_seasons_transfers'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(wyId String,
transfer_active String,
transfer_announceDate String,
transfer_currency String,
transfer_endDate String,
transfer_fromTeamId String,
transfer_fromTeamName String,
transfer_playerId String,
transfer_startDate String,
transfer_toTeamId String,
transfer_toTeamName String,
transfer_type String,
transfer_value String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_seasons_transfers',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_details',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_details'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(birthDate String,
currentNationalTeamId String,
currentTeamId String,
firstName String,
foot String,
gender String,
height String,
imageDataURL String,
lastName String,
middleName String,
shortName String,
status String,
weight String,
wyId String,
birthArea_alpha2code String,
birthArea_alpha3code String,
birthArea_id String,
birthArea_name String,
passportArea_alpha2code String,
passportArea_alpha3code String,
passportArea_id String,
passportArea_name String,
role_code2 String,
role_code3 String,
role_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_details',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players_transfers tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_transfers',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_transfers'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(wyId String,
transfer_active String,
transfer_announceDate String,
transfer_currency String,
transfer_endDate String,
transfer_fromTeamId String,
transfer_fromTeamName String,
transfer_playerId String,
transfer_startDate String,
transfer_toTeamId String,
transfer_toTeamName String,
transfer_transferId String,
transfer_type String,
transfer_value String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_transfers',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players_matches tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_matches',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_matches'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matches_competitionId String ,
matches_date String,
matches_dateutc String,
matches_gameweek String,
matches_label String,
matches_matchId String,
matches_roundId String,
matches_seasonId String,
matches_status String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_matches',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players_contract_info tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_contract_info',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_contract_info'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(agencies String, 
contractExpiration String, 
playerId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_contract_info',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players_fixtures tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_fixtures',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_fixtures'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matches_competitionId String,
matches_date String,
matches_dateutc String,
matches_gameweek String,
matches_label String,
matches_matchId String,
matches_roundId String,
matches_seasonId String,
matches_status String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_fixtures',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players_career tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_career',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_career'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(career_appearances String ,
career_competitionId String,
career_goal String,
career_minutesPlayed String,
career_penalties String,
career_playerId String,
career_redCards String,
career_seasonId String,
career_shirtNumber String,
career_substituteIn String,
career_substituteOnBench String,
career_substituteOut String,
career_teamId String,
career_yellowCard String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_career',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_matches_details tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_matches_details',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_matches_details'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String ,
date String,
dateutc String,
duration String,
gameweek String,
hasDataAvailable String,
label String,
roundId String,
seasonId String,
status String,
venue String,
winner String,
wyId String,
referees_refereeId String,
referees_role String,
teamsData_away_coachId String,
teamsData_away_hasFormation String,
teamsData_away_score String,
teamsData_away_scoreET String,
teamsData_away_scoreHT String,
teamsData_away_scoreP String,
teamsData_away_side String,
teamsData_away_teamId String,
teamsData_home_coachId String,
teamsData_home_hasFormation String,
teamsData_home_score String,
teamsData_home_scoreET String,
teamsData_home_scoreHT String,
teamsData_home_scoreP String,
teamsData_home_side String,
teamsData_home_teamId String,
teamsData_away_formation_bench_assists String,
teamsData_away_formation_bench_goals String,
teamsData_away_formation_bench_ownGoals String,
teamsData_away_formation_bench_playerId String,
teamsData_away_formation_bench_redCards String,
teamsData_away_formation_bench_shirtNumber String,
teamsData_away_formation_bench_yellowCards String,
teamsData_away_formation_lineup_assists String,
teamsData_away_formation_lineup_goals String,
teamsData_away_formation_lineup_ownGoals String,
teamsData_away_formation_lineup_playerId String,
teamsData_away_formation_lineup_redCards String,
teamsData_away_formation_lineup_shirtNumber String,
teamsData_away_formation_lineup_yellowCards String,
teamsData_away_formation_substitutions_assists String,
teamsData_away_formation_substitutions_minute String,
teamsData_away_formation_substitutions_playerIn String,
teamsData_away_formation_substitutions_playerOut String,
teamsData_home_formation_bench_assists String,
teamsData_home_formation_bench_goals String,
teamsData_home_formation_bench_ownGoals String,
teamsData_home_formation_bench_playerId String,
teamsData_home_formation_bench_redCards String,
teamsData_home_formation_bench_shirtNumber String,
teamsData_home_formation_bench_yellowCards String,
teamsData_home_formation_lineup_assists String,
teamsData_home_formation_lineup_goals String,
teamsData_home_formation_lineup_ownGoals String,
teamsData_home_formation_lineup_playerId String,
teamsData_home_formation_lineup_redCards String,
teamsData_home_formation_lineup_shirtNumber String,
teamsData_home_formation_lineup_yellowCards String,
teamsData_home_formation_substitutions_assists String,
teamsData_home_formation_substitutions_minute String,
teamsData_home_formation_substitutions_playerIn String,
teamsData_home_formation_substitutions_playerOut String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_matches_details',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_matches_formations tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_matches_formations',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_matches_formations'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(home_id String,
home_scheme String,
home_startSec String,
home_endSec String,
home_matchPeriodStart String,
home_matchPeriodEnd String,
home_teamId String,
home_playersOnField String,
home_players_playerId String,
home_players_position String,
away_id String,
away_scheme String,
away_startSec String,
away_endSec String,
away_matchPeriodStart String,
away_matchPeriodEnd String,
away_teamId String,
away_playersOnField String,
away_players_playerId String,
away_players_position String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_matches_formations',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_matches_directions tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_matches_directions',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_matches_directions'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(1H_leftToRightTeamId String,
1H_rightToLeftTeamId String,
2H_leftToRightTeamId String,
2H_rightToLeftTeamId String,
E1_leftToRightTeamId String,
E1_rightToLeftTeamId String,
E2_leftToRightTeamId String,
E2_rightToLeftTeamId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_matches_directions',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_matches_fulltracking tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_matches_fulltracking',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_matches_fulltracking'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(trackingDownloadUrl String,
wyId String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_matches_fulltracking',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_videos_qualities tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_videos_qualities',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_videos_qualities'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matchId String ,
isAvailable String,
hd_isAvailable String,
lq_isAvailable String,
sd_isAvailable String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_videos_qualities',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players_matches_advanced_stats tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_matches_advanced_stats',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_matches_advanced_stats'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String,
matchId String,
playerId String,
roundId String,
seasonId String,
average_accelerations String,
average_aerialDuels String,
average_assists String,
average_attackingActions String,
average_backPasses String,
average_ballLosses String,
average_ballRecoveries String,
average_clearances String,
average_corners String,
average_counterpressingRecoveries String,
average_crosses String,
average_dangerousOpponentHalfRecoveries String,
average_dangerousOwnHalfLosses String,
average_defensiveActions String,
average_defensiveDuels String,
average_defensiveDuelsWon String,
average_directFreeKicks String,
average_directFreeKicksOnTarget String,
average_directRedCards String,
average_dribbleDistanceFromOpponentGoal String,
average_dribbles String,
average_dribblesAgainst String,
average_dribblesAgainstWon String,
average_duels String,
average_duelsWon String,
average_fieldAerialDuels String,
average_fieldAerialDuelsWon String,
average_forwardPasses String,
average_fouls String,
average_foulsSuffered String,
average_freeKicks String,
average_freeKicksOnTarget String,
average_gkAerialDuels String,
average_gkAerialDuelsWon String,
average_gkConcededGoals String,
average_gkExits String,
average_gkSaves String,
average_gkShotsAgainst String,
average_gkSuccessfulExits String,
average_goalKicks String,
average_goalKicksLong String,
average_goalKicksShort String,
average_goals String,
average_headShots String,
average_interceptions String,
average_keyPasses String,
average_lateralPasses String,
average_linkupPlays String,
average_longPassLength String,
average_longPasses String,
average_looseBallDuels String,
average_looseBallDuelsWon String,
average_losses String,
average_missedBalls String,
average_newDefensiveDuelsWon String,
average_newDuelsWon String,
average_newOffensiveDuelsWon String,
average_newSuccessfulDribbles String,
average_offensiveDuels String,
average_offensiveDuelsWon String,
average_offsides String,
average_opponentHalfRecoveries String,
average_ownHalfLosses String,
average_passLength String,
average_passes String,
average_passesToFinalThird String,
average_penalties String,
average_progressivePasses String,
average_progressiveRun String,
average_receivedPass String,
average_redCards String,
average_secondAssists String,
average_shotAssists String,
average_shotOnTargetAssists String,
average_shots String,
average_shotsBlocked String,
average_shotsOnTarget String,
average_slidingTackles String,
average_smartPasses String,
average_successfulAttackingActions String,
average_successfulBackPasses String,
average_successfulCrosses String,
average_successfulDefensiveAction String,
average_successfulDribbles String,
average_successfulForwardPasses String,
average_successfulGoalKicks String,
average_successfulKeyPasses String,
average_successfulLateralPasses String,
average_successfulLinkupPlays String,
average_successfulLongPasses String,
average_successfulPasses String,
average_successfulPassesToFinalThird String,
average_successfulPenalties String,
average_successfulProgressivePasses String,
average_successfulSlidingTackles String,
average_successfulSmartPasses String,
average_successfulThroughPasses String,
average_successfulVerticalPasses String,
average_thirdAssists String,
average_throughPasses String,
average_touchInBox String,
average_verticalPasses String,
average_xgAssist String,
average_xgSave String,
average_xgShot String,
average_yellowCards String,
percent_aerialDuelsWon String,
percent_defensiveDuelsWon String,
percent_directFreeKicksOnTarget String,
percent_dribblesAgainstWon String,
percent_duelsWon String,
percent_fieldAerialDuelsWon String,
percent_gkAerialDuelsWon String,
percent_gkSaves String,
percent_gkSuccessfulExits String,
percent_goalConversion String,
percent_headShotsOnTarget String,
percent_newDefensiveDuelsWon String,
percent_newDuelsWon String,
percent_newOffensiveDuelsWon String,
percent_newSuccessfulDribbles String,
percent_offensiveDuelsWon String,
percent_penaltiesConversion String,
percent_shotsOnTarget String,
percent_successfulBackPasses String,
percent_successfulCrosses String,
percent_successfulDribbles String,
percent_successfulForwardPasses String,
percent_successfulGoalKicks String,
percent_successfulKeyPasses String,
percent_successfulLateralPasses String,
percent_successfulLinkupPlays String,
percent_successfulLongPasses String,
percent_successfulPasses String,
percent_successfulPassesToFinalThird String,
percent_successfulProgressivePasses String,
percent_successfulShotAssists String,
percent_successfulSlidingTackles String,
percent_successfulSmartPasses String,
percent_successfulThroughPasses String,
percent_successfulVerticalPasses String,
percent_win String,
percent_yellowCardsPerFoul String,
positions_percent String,
total_accelerations String,
total_aerialDuels String,
total_aerialDuelsWon String,
total_assists String,
total_attackingActions String,
total_backPasses String,
total_clearances String,
total_corners String,
total_counterpressingRecoveries String,
total_crosses String,
total_dangerousOpponentHalfRecoveries String,
total_dangerousOwnHalfLosses String,
total_defensiveActions String,
total_defensiveDuels String,
total_defensiveDuelsWon String,
total_directFreeKicks String,
total_directFreeKicksOnTarget String,
total_directRedCards String,
total_dribbles String,
total_dribblesAgainst String,
total_dribblesAgainstWon String,
total_duels String,
total_duelsWon String,
total_fieldAerialDuels String,
total_fieldAerialDuelsWon String,
total_forwardPasses String,
total_fouls String,
total_foulsSuffered String,
total_freeKicks String,
total_freeKicksOnTarget String,
total_gkAerialDuels String,
total_gkAerialDuelsWon String,
total_gkCleanSheets String,
total_gkConcededGoals String,
total_gkExits String,
total_gkSaves String,
total_gkShotsAgainst String,
total_gkSuccessfulExits String,
total_goalKicks String,
total_goalKicksLong String,
total_goalKicksShort String,
total_goals String,
total_headShots String,
total_interceptions String,
total_keyPasses String,
total_lateralPasses String,
total_linkupPlays String,
total_longPasses String,
total_looseBallDuels String,
total_looseBallDuelsWon String,
total_losses String,
total_matches String,
total_matchesComingOff String,
total_matchesInStart String,
total_matchesSubstituted String,
total_minutesOnField String,
total_minutesTagged String,
total_missedBalls String,
total_newDefensiveDuelsWon String,
total_newDuelsWon String,
total_newOffensiveDuelsWon String,
total_newSuccessfulDribbles String,
total_offensiveDuels String,
total_offensiveDuelsWon String,
total_offsides String,
total_opponentHalfRecoveries String,
total_ownHalfLosses String,
total_passes String,
total_passesToFinalThird String,
total_penalties String,
total_pressingDuels String,
total_pressingDuelsWon String,
total_progressivePasses String,
total_progressiveRun string,
total_receivedPass String,
total_recoveries String,
total_redCards String,
total_secondAssists String,
total_shotAssists String,
total_shotOnTargetAssists String,
total_shots String,
total_shotsBlocked String,
total_shotsOnTarget String,
total_slidingTackles String,
total_smartPasses String,
total_successfulAttackingActions String,
total_successfulBackPasses String,
total_successfulCrosses String,
total_successfulDefensiveAction String,
total_successfulDribbles String,
total_successfulForwardPasses String,
total_successfulGoalKicks String,
total_successfulKeyPasses String,
total_successfulLateralPasses String,
total_successfulLinkupPlays String,
total_successfulLongPasses String,
total_successfulPasses String,
total_successfulPassesToFinalThird String,
total_successfulPenalties String,
total_successfulProgressivePasses String,
total_successfulSlidingTackles String,
total_successfulSmartPasses String,
total_successfulThroughPasses String,
total_successfulVerticalPasses String,
total_thirdAssists String,
total_throughPasses String,
total_touchInBox String,
total_verticalPasses String,
total_xgAssist String,
total_xgSave String,
total_xgShot String,
total_yellowCards String,
positions_position_code String,
positions_position_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_matches_advanced_stats',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_players_advanced_stats tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_players_advanced_stats',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_players_advanced_stats'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(competitionId String ,
playerId String,
positions String,
roundId String,
seasonId String,
average_accelerations String,
average_aerialDuels String,
average_assists String,
average_attackingActions String,
average_backPasses String,
average_ballLosses String,
average_ballRecoveries String,
average_clearances String,
average_corners String,
average_counterpressingRecoveries String,
average_crosses String,
average_dangerousOpponentHalfRecoveries String,
average_dangerousOwnHalfLosses String,
average_defensiveActions String,
average_defensiveDuels String,
average_defensiveDuelsWon String,
average_directFreeKicks String,
average_directFreeKicksOnTarget String,
average_directRedCards String,
average_dribbleDistanceFromOpponentGoal String,
average_dribbles String,
average_dribblesAgainst String,
average_dribblesAgainstWon String,
average_duels String,
average_duelsWon String,
average_fieldAerialDuels String,
average_fieldAerialDuelsWon String,
average_forwardPasses String,
average_fouls String,
average_foulsSuffered String,
average_freeKicks String,
average_freeKicksOnTarget String,
average_gkAerialDuels String,
average_gkAerialDuelsWon String,
average_gkConcededGoals String,
average_gkExits String,
average_gkSaves String,
average_gkShotsAgainst String,
average_gkSuccessfulExits String,
average_goalKicks string,
average_goalKicksLong String,
average_goalKicksShort String,
average_goals String,
average_headShots String,
average_interceptions String,
average_keyPasses String,
average_lateralPasses String,
average_linkupPlays String,
average_longPassLength String,
average_longPasses String,
average_looseBallDuels String,
average_looseBallDuelsWon String,
average_losses String,
average_missedBalls String,
average_newDefensiveDuelsWon String,
average_newDuelsWon String,
average_newOffensiveDuelsWon String,
average_newSuccessfulDribbles String,
average_offensiveDuels String,
average_offensiveDuelsWon String,
average_offsides String,
average_opponentHalfRecoveries String,
average_ownHalfLosses String,
average_passLength String,
average_passes String,
average_passesToFinalThird String,
average_penalties String,
average_progressivePasses String,
average_progressiveRun String,
average_receivedPass String,
average_redCards String,
average_secondAssists String,
average_shotAssists String,
average_shotOnTargetAssists String,
average_shots String,
average_shotsBlocked String,
average_shotsOnTarget String,
average_slidingTackles String,
average_smartPasses String,
average_successfulAttackingActions String,
average_successfulBackPasses String,
average_successfulCrosses String,
average_successfulDefensiveAction String,
average_successfulDribbles String,
average_successfulForwardPasses String,
average_successfulGoalKicks String,
average_successfulKeyPasses String,
average_successfulLateralPasses String,
average_successfulLinkupPlays String,
average_successfulLongPasses String,
average_successfulPasses String,
average_successfulPassesToFinalThird String,
average_successfulPenalties String,
average_successfulProgressivePasses String,
average_successfulSlidingTackles String,
average_successfulSmartPasses string,
average_successfulThroughPasses String,
average_successfulVerticalPasses String,
average_thirdAssists String,
average_throughPasses String,
average_touchInBox String,
average_verticalPasses String,
average_xgAssist String,
average_xgSave String,
average_xgShot String,
average_yellowCards String,
percent_aerialDuelsWon String,
percent_defensiveDuelsWon String,
percent_directFreeKicksOnTarget String,
percent_dribblesAgainstWon String,
percent_duelsWon String,
percent_fieldAerialDuelsWon String,
percent_gkAerialDuelsWon String,
percent_gkSaves String,
percent_gkSuccessfulExits String,
percent_goalConversion String,
percent_headShotsOnTarget String,
percent_newDefensiveDuelsWon String,
percent_newDuelsWon String,
percent_newOffensiveDuelsWon String,
percent_newSuccessfulDribbles String,
percent_offensiveDuelsWon String,
percent_penaltiesConversion String,
percent_shotsOnTarget String,
percent_successfulBackPasses String,
percent_successfulCrosses String,
percent_successfulDribbles String,
percent_successfulForwardPasses String,
percent_successfulGoalKicks String,
percent_successfulKeyPasses String,
percent_successfulLateralPasses String,
percent_successfulLinkupPlays String,
percent_successfulLongPasses String,
percent_successfulPasses String,
percent_successfulPassesToFinalThird String,
percent_successfulProgressivePasses string,
percent_successfulShotAssists String,
percent_successfulSlidingTackles String,
percent_successfulSmartPasses String,
percent_successfulThroughPasses String,
percent_successfulVerticalPasses String,
percent_win String,
percent_yellowCardsPerFoul String,
total_accelerations String,
total_aerialDuels String,
total_aerialDuelsWon String,
total_assists String,
total_attackingActions String,
total_backPasses String,
total_clearances String,
total_corners String,
total_counterpressingRecoveries String,
total_crosses String,
total_dangerousOpponentHalfRecoveries String,
total_dangerousOwnHalfLosses String,
total_defensiveActions String,
total_defensiveDuels String,
total_defensiveDuelsWon String,
total_directFreeKicks String,
total_directFreeKicksOnTarget String,
total_directRedCards String,
total_dribbles String,
total_dribblesAgainst String,
total_dribblesAgainstWon String,
total_duels String,
total_duelsWon String,
total_fieldAerialDuels String,
total_fieldAerialDuelsWon String,
total_forwardPasses String,
total_fouls String,
total_foulsSuffered String,
total_freeKicks String,
total_freeKicksOnTarget String,
total_gkAerialDuels String,
total_gkAerialDuelsWon String,
total_gkCleanSheets String,
total_gkConcededGoals String,
total_gkExits String,
total_gkSaves String,
total_gkShotsAgainst String,
total_gkSuccessfulExits String,
total_goalKicks String,
total_goalKicksLong String,
total_goalKicksShort String,
total_goals String,
total_headShots String,
total_interceptions String,
total_keyPasses String,
total_lateralPasses String,
total_linkupPlays String,
total_longPasses String,
total_looseBallDuels String,
total_looseBallDuelsWon String,
total_losses String,
total_matches String,
total_matchesComingOff String,
total_matchesInStart String,
total_matchesSubstituted String,
total_minutesOnField String,
total_minutesTagged String,
total_missedBalls String,
total_newDefensiveDuelsWon String,
total_newDuelsWon String,
total_newOffensiveDuelsWon String,
total_newSuccessfulDribbles String,
total_offensiveDuels String,
total_offensiveDuelsWon String,
total_offsides String,
total_opponentHalfRecoveries String,
total_ownHalfLosses String,
total_passes String,
total_passesToFinalThird String,
total_penalties String,
total_pressingDuels String,
total_pressingDuelsWon String,
total_progressivePasses String,
total_progressiveRun String,
total_receivedPass String,
total_recoveries String,
total_redCards String,
total_secondAssists String,
total_shotAssists String,
total_shotOnTargetAssists String,
total_shots String,
total_shotsBlocked String,
total_shotsOnTarget String,
total_slidingTackles String,
total_smartPasses String,
total_successfulAttackingActions String,
total_successfulBackPasses String,
total_successfulCrosses String,
total_successfulDefensiveAction String,
total_successfulDribbles String,
total_successfulForwardPasses String,
total_successfulGoalKicks String,
total_successfulKeyPasses String,
total_successfulLateralPasses String,
total_successfulLinkupPlays String,
total_successfulLongPasses String,
total_successfulPasses String,
total_successfulPassesToFinalThird String,
total_successfulPenalties String,
total_successfulProgressivePasses String,
total_successfulSlidingTackles String,
total_successfulSmartPasses string,
total_successfulThroughPasses String,
total_successfulVerticalPasses String,
total_thirdAssists String,
total_throughPasses String,
total_touchInBox String,
total_verticalPasses String,
total_xgAssist String,
total_xgSave String,
total_xgShot String,
total_yellowCards String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_players_advanced_stats',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_matches_events tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_matches_events',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_matches_events'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(meta String ,
events_id String,
events_matchId String,
events_matchPeriod String,
events_matchTimestamp String,
events_minute String,
relatedEventId String,
events_second String,
events_videoTimestamp String,
events_aerialDuel_firstTouch String,
events_aerialDuel_height String,
events_aerialDuel_relatedDuelId String,
events_carry_progression String,
events_groundDuel_duelType String,
events_groundDuel_keptPossession String,
events_groundDuel_progressedWithBall String,
events_groundDuel_recoveredPossession String,
events_groundDuel_relatedDuelId String,
events_groundDuel_side String,
events_groundDuel_stoppedProgress String,
events_groundDuel_takeOn String,
events_infraction_redCard String,
events_infraction_type String,
events_infraction_yellowCard String,
events_location_x String,
events_location_y String,
events_opponentTeam_formation String,
events_opponentTeam_id String,
events_opponentTeam_name String,
events_pass_accurate String,
events_pass_angle String,
events_pass_height String,
pass_length String,
events_player_id String,
events_player_name String,
events_player_position String,
events_possession_duration String,
events_possession_eventIndex String,
events_possession_eventsNumber String,
events_possession_id String,
events_possession_types String,
events_shot_bodyPart String,
events_shot_goalZone String,
events_shot_goalkeeperActionId string,
events_shot_isGoal String,
events_shot_onTarget String,
events_shot_postShotXg String,
events_shot_xg String,
events_team_formation String,
events_team_id String,
events_team_name String,
events_type_primary String,
events_type_secondary String,
events_aerialDuel_opponent_height String,
events_aerialDuel_opponent_id String,
events_aerialDuel_opponent_name String,
events_aerialDuel_opponent_position String,
events_carry_endLocation_x String,
events_carry_endLocation_y String,
events_groundDuel_opponent_id String,
events_groundDuel_opponent_name String,
events_groundDuel_opponent_position String,
events_infraction_opponent_id String,
events_infraction_opponent_name String,
events_infraction_opponent_position String,
events_pass_endLocation_x String,
events_pass_endLocation_y String,
events_pass_recipient_id String,
events_pass_recipient_name String,
events_pass_recipient_position String,
events_possession_attack_flank String,
events_possession_attack_withGoal String,
events_possession_attack_withShot String,
events_possession_attack_withShotOnGoal String,
events_possession_attack_xg String,
events_possession_endLocation_x String,
events_possession_endLocation_y String,
events_possession_startLocation_x String,
events_possession_startLocation_y String,
events_possession_team_formation String,
events_possession_team_id String,
events_possession_team_name String,
events_shot_goalkeeper_id String,
events_shot_goalkeeper_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_matches_events',STANDARIZED_RAW_CONTAINER))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_matches_player_advanced_stats tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_matches_player_advanced_stats',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_matches_player_advanced_stats'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(players_competitionId String,
players_matchId String,
players_playerId String,
players_roundId String,
players_seasonId String,
players_average_accelerations String,
players_average_aerialDuels String,
players_average_assists String,
players_average_attackingActions String,
players_average_backPasses String,
players_average_ballLosses String,
players_average_ballRecoveries String,
players_average_clearances String,
players_average_corners String,
players_average_counterpressingRecoveries String,
players_average_crosses String,
players_average_dangerousOpponentHalfRecoveries String,
players_average_dangerousOwnHalfLosses String,
players_average_defensiveActions String,
players_average_defensiveDuels String,
players_average_defensiveDuelsWon String,
players_average_directFreeKicks String,
players_average_directFreeKicksOnTarget String,
players_average_directRedCards String,
players_average_dribbleDistanceFromOpponentGoal String,
players_average_dribbles String,
players_average_dribblesAgainst String,
players_average_dribblesAgainstWon String,
players_average_duels String,
players_average_duelsWon String,
players_average_fieldAerialDuels String,
players_average_fieldAerialDuelsWon String,
players_average_forwardPasses String,
players_average_fouls String,
players_average_foulsSuffered String,
players_average_freeKicks String,
players_average_freeKicksOnTarget String,
players_average_gkAerialDuels String,
players_average_gkAerialDuelsWon String,
players_average_gkConcededGoals String,
players_average_gkExits String,
players_average_gkSaves String,
players_average_gkShotsAgainst String,
players_average_gkSuccessfulExits String,
players_average_goalKicks String,
players_average_goalKicksLong String,
players_average_goalKicksShort String,
players_average_goals String,
players_average_headShots String,
players_average_interceptions String,
players_average_keyPasses String,
players_average_lateralPasses String,
players_average_linkupPlays String,
players_average_longPassLength String,
players_average_longPasses String,
players_average_looseBallDuels String,
players_average_looseBallDuelsWon String,
players_average_losses String,
players_average_missedBalls String,
players_average_newDefensiveDuelsWon String,
players_average_newDuelsWon String,
players_average_newOffensiveDuelsWon String,
players_average_newSuccessfulDribbles String,
players_average_offensiveDuels String,
players_average_offensiveDuelsWon String,
players_average_offsides String,
players_average_opponentHalfRecoveries String,
players_average_ownHalfLosses String,
players_average_passLength String,
players_average_passes String,
players_average_passesToFinalThird String,
players_average_penalties String,
players_average_progressivePasses String,
players_average_progressiveRun String,
players_average_receivedPass String,
players_average_redCards String,
players_average_secondAssists String,
players_average_shotAssists String,
players_average_shotOnTargetAssists String,
players_average_shots String,
players_average_shotsBlocked String,
players_average_shotsOnTarget String,
players_average_slidingTackles String,
players_average_smartPasses String,
players_average_successfulAttackingActions String,
players_average_successfulBackPasses String,
players_average_successfulCrosses String,
players_average_successfulDefensiveAction String,
players_average_successfulDribbles String,
players_average_successfulForwardPasses String,
players_average_successfulGoalKicks String,
players_average_successfulKeyPasses String,
players_average_successfulLateralPasses String,
players_average_successfulLinkupPlays String,
players_average_successfulLongPasses String,
players_average_successfulPasses String,
players_average_successfulPassesToFinalThird String,
players_average_successfulPenalties String,
players_average_successfulProgressivePasses String,
players_average_successfulSlidingTackles String,
players_average_successfulSmartPasses String,
players_average_successfulThroughPasses String,
players_average_successfulVerticalPasses String,
players_average_thirdAssists String,
players_average_throughPasses String,
players_average_touchInBox String,
players_average_verticalPasses String,
players_average_xgAssist String,
players_average_xgSave String,
players_average_xgShot String,
players_average_yellowCards String,
players_percent_aerialDuelsWon String,
players_percent_defensiveDuelsWon String,
players_percent_directFreeKicksOnTarget String,
players_percent_dribblesAgainstWon String,
players_percent_duelsWon String,
players_percent_fieldAerialDuelsWon String,
players_percent_gkAerialDuelsWon string,
players_percent_gkSaves String,
players_percent_gkSuccessfulExits String,
players_percent_goalConversion String,
players_percent_headShotsOnTarget String,
players_percent_newDefensiveDuelsWon String,
players_percent_newDuelsWon String,
players_percent_newOffensiveDuelsWon String,
players_percent_newSuccessfulDribbles String,
players_percent_offensiveDuelsWon String,
players_percent_penaltiesConversion String,
players_percent_shotsOnTarget String,
players_percent_successfulBackPasses String,
players_percent_successfulCrosses String,
players_percent_successfulDribbles String,
players_percent_successfulForwardPasses String,
players_percent_successfulGoalKicks String,
players_percent_successfulKeyPasses String,
players_percent_successfulLateralPasses string,
players_percent_successfulLinkupPlays String,
players_percent_successfulLongPasses String,
players_percent_successfulPasses String,
players_percent_successfulPassesToFinalThird String,
players_percent_successfulProgressivePasses String,
players_percent_successfulShotAssists String,
players_percent_successfulSlidingTackles String,
players_percent_successfulSmartPasses String,
players_percent_successfulThroughPasses String,
players_percent_successfulVerticalPasses String,
players_percent_win String,
players_percent_yellowCardsPerFoul String,
players_positions_percent String,
players_total_accelerations String,
players_total_aerialDuels String,
players_total_aerialDuelsWon String,
players_total_assists String,
players_total_attackingActions String,
players_total_backPasses String,
players_total_clearances String,
players_total_corners String,
players_total_counterpressingRecoveries String,
players_total_crosses String,
players_total_dangerousOpponentHalfRecoveries String,
players_total_dangerousOwnHalfLosses String,
players_total_defensiveActions String,
players_total_defensiveDuels String,
players_total_defensiveDuelsWon String,
players_total_directFreeKicks String,
players_total_directFreeKicksOnTarget String,
players_total_directRedCards String,
players_total_dribbles String,
players_total_dribblesAgainst String,
players_total_dribblesAgainstWon String,
players_total_duels String,
players_total_duelsWon String,
players_total_fieldAerialDuels String,
players_total_fieldAerialDuelsWon String,
players_total_forwardPasses String,
players_total_fouls String,
players_total_foulsSuffered String,
players_total_freeKicks String,
players_total_freeKicksOnTarget String,
players_total_gkAerialDuels String,
players_total_gkAerialDuelsWon String,
players_total_gkCleanSheets String,
players_total_gkConcededGoals String,
players_total_gkExits String,
players_total_gkSaves String,
players_total_gkShotsAgainst String,
players_total_gkSuccessfulExits String,
players_total_goalKicks String,
players_total_goalKicksLong String,
players_total_goalKicksShort String,
players_total_goals String,
players_total_headShots String,
players_total_interceptions String,
players_total_keyPasses String,
players_total_lateralPasses String,
players_total_linkupPlays String,
players_total_longPasses String,
players_total_looseBallDuels String,
players_total_looseBallDuelsWon String,
players_total_losses String,
players_total_matches String,
players_total_matchesComingOff String,
players_total_matchesInStart String,
players_total_matchesSubstituted String,
players_total_minutesOnField String,
players_total_minutesTagged String,
players_total_missedBalls String,
players_total_newDefensiveDuelsWon String,
players_total_newDuelsWon String,
players_total_newOffensiveDuelsWon String,
players_total_newSuccessfulDribbles String,
players_total_offensiveDuels String,
players_total_offensiveDuelsWon String,
players_total_offsides String,
players_total_opponentHalfRecoveries String,
players_total_ownHalfLosses String,
players_total_passes String,
players_total_passesToFinalThird String,
players_total_penalties String,
players_total_pressingDuels String,
players_total_pressingDuelsWon String,
players_total_progressivePasses String,
players_total_progressiveRun String,
players_total_receivedPass String,
players_total_recoveries String,
players_total_redCards String,
players_total_secondAssists String,
players_total_shotAssists String,
players_total_shotOnTargetAssists String,
players_total_shots String,
players_total_shotsBlocked String,
players_total_shotsOnTarget String,
players_total_slidingTackles String,
players_total_smartPasses String,
players_total_successfulAttackingActions String,
players_total_successfulBackPasses String,
players_total_successfulCrosses String,
players_total_successfulDefensiveAction String,
players_total_successfulDribbles String,
players_total_successfulForwardPasses String,
players_total_successfulGoalKicks String,
players_total_successfulKeyPasses String,
players_total_successfulLateralPasses String,
players_total_successfulLinkupPlays String,
players_total_successfulLongPasses String,
players_total_successfulPasses String,
players_total_successfulPassesToFinalThird String,
players_total_successfulPenalties String,
players_total_successfulProgressivePasses String,
players_total_successfulSlidingTackles String,
players_total_successfulSmartPasses String,
players_total_successfulThroughPasses String,
players_total_successfulVerticalPasses String,
players_total_thirdAssists String,
players_total_throughPasses String,
players_total_touchInBox String,
players_total_verticalPasses String,
players_total_xgAssist String,
players_total_xgSave String,
players_total_xgShot String,
players_total_yellowCards String,
players_positions_position_code String,
players_positions_position_name String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_matches_player_advanced_stats',STANDARIZED_RAW_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Init wyscout_matches_advanced_stats tables        ##############################################
###########################################################################################
###########################################################################################

dbutils.fs.rm("{1}/{0}/".format('wyscout_matches_advanced_stats',STANDARIZED_RAW_CONTAINER),True)

sqlContext.sql("""DROP TABLE IF EXISTS {0}.standarized_raw.{1}""".format(BRONZE_CATALOG_NAME,'wyscout_matches_advanced_stats'))

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.standarized_raw.{1} 
(matchId String,
possession_deadTime String,
possession_totalTime String,
attacks_away_corners String,
attacks_away_cornersWithShot String,
attacks_away_counterAttacks String,
attacks_away_freeKicks String,
attacks_away_freeKicksWithShot String,
attacks_away_positionalAttack String,
attacks_away_positionalWithShots String,
attacks_away_total String,
attacks_away_withShots String,
attacks_home_corners String,
attacks_home_cornersWithShot String,
attacks_home_counterAttacks String,
attacks_home_freeKicks string,
attacks_home_freeKicksWithShot String,
attacks_home_positionalAttack String,
attacks_home_positionalWithShots string,
attacks_home_total String,
attacks_home_withShots string,
defence_away_clearances string,
defence_away_interceptions String,
defence_away_ppda String,
defence_away_tackles string,
defence_home_clearances string,
defence_home_interceptions string,
defence_home_ppda String,
defence_home_tackles string,
duels_away_aerialDuels String,
duels_away_aerialDuelsSuccessful String,
duels_away_challengeIntensity String,
duels_away_defensiveDuels String,
duels_away_defensiveDuelsSuccessful String,
duels_away_dribbles String,
duels_away_dribblesSuccessful String,
duels_away_duels String,
duels_away_duelsSuccessful String,
duels_away_groundDuels String,
duels_away_groundDuelsSuccessful String,
duels_away_looseBallDuels String,
duels_away_looseBallDuelsSuccessful String,
duels_away_offensiveDuels String,
duels_away_offensiveDuelsSuccessful String,
duels_home_aerialDuels String,
duels_home_aerialDuelsSuccessful String,
duels_home_challengeIntensity String,
duels_home_defensiveDuels String,
duels_home_defensiveDuelsSuccessful String,
duels_home_dribbles String,
duels_home_dribblesSuccessful String,
duels_home_duels String,
duels_home_duelsSuccessful String,
duels_home_groundDuels String,
duels_home_groundDuelsSuccessful String,
duels_home_looseBallDuels String,
duels_home_looseBallDuelsSuccessful String,
duels_home_offensiveDuels String,
duels_home_offensiveDuelsSuccessful String,
flanks_away_centerAttacks string,
flanks_away_centerXg String,
flanks_away_leftFlankAttacks String,
flanks_away_leftFlankXg String,
flanks_away_rightFlankAttacks String,
flanks_away_rightFlankXg String,
flanks_home_centerAttacks string,
flanks_home_centerXg String,
flanks_home_leftFlankAttacks String,
flanks_home_leftFlankXg String,
flanks_home_rightFlankAttacks String,
flanks_home_rightFlankXg String,
general_away_avgDistance String,
general_away_corners String,
general_away_dribbles String,
general_away_fouls String,
general_away_foulsSuffered String,
general_away_freeKicks String,
general_away_goals String,
general_away_leftThrowIns String,
general_away_offsides String,
general_away_progressiveRuns String,
general_away_redCards String,
general_away_rightThrowIns String,
general_away_shots String,
general_away_shotsBlocked String,
general_away_shotsFromBox String,
general_away_shotsFromBoxOnTarget String,
general_away_shotsFromDangerZone String,
general_away_shotsOnPost String,
general_away_shotsOnTarget String,
general_away_shotsOutsideBox String,
general_away_shotsOutsideBoxOnTarget String,
general_away_shotsWide String,
general_away_totalThrowIns String,
general_away_touchesInBox String,
general_away_xg String,
general_away_xgPerShot String,
general_away_yellowCards String,
general_home_avgDistance String,
general_home_corners String,
general_home_dribbles String,
general_home_fouls String,
general_home_foulsSuffered String,
general_home_freeKicks String,
general_home_goals String,
general_home_leftThrowIns String,
general_home_offsides String,
general_home_progressiveRuns String,
general_home_redCards String,
general_home_rightThrowIns String,
general_home_shots String,
general_home_shotsBlocked String,
general_home_shotsFromBox String,
general_home_shotsFromBoxOnTarget String,
general_home_shotsFromDangerZone String,
general_home_shotsOnPost String,
general_home_shotsOnTarget String,
general_home_shotsOutsideBox String,
general_home_shotsOutsideBoxOnTarget String,
general_home_shotsWide String,
general_home_totalThrowIns String,
general_home_touchesInBox String,
general_home_xg String,
general_home_xgPerShot String,
general_home_yellowCards String,
openPlay_away_long String,
openPlay_away_medium String,
openPlay_away_short String,
openPlay_away_total String,
openPlay_away_veryLong String,
openPlay_home_long String,
openPlay_home_medium String,
openPlay_home_short String,
openPlay_home_total String,
openPlay_home_veryLong String,
passes_away_assists string,
passes_away_avgPassLength String,
passes_away_avgPassToFinalThirdLength String,
passes_away_backPasses String,
passes_away_backPassesSuccessful String,
passes_away_crossesBlocked String,
passes_away_crossesFromLeftFlank String,
passes_away_crossesFromLeftFlankSuccessful String,
passes_away_crossesFromRightFlank String,
passes_away_crossesFromRightFlankSuccessful String,
passes_away_crossesHigh String,
passes_away_crossesLow String,
passes_away_crossesSuccessful String,
passes_away_crossesTotal String,
passes_away_deepCompletedPasses String,
passes_away_deepCompletedPassesSuccessful String,
passes_away_forwardPasses String,
passes_away_forwardPassesSuccessful String,
passes_away_keyPasses String,
passes_away_keyPassesSuccessful String,
passes_away_lateralPasses String,
passes_away_lateralPassesSuccessful String,
passes_away_longPasses String,
passes_away_longPassesSuccessful String,
passes_away_matchTempo String,
passes_away_passToFinalThirds String,
passes_away_passToFinalThirdsSuccessful String,
passes_away_passToPenaltyAreas String,
passes_away_passToPenaltyAreasSuccessful String,
passes_away_passes String,
passes_away_passesSuccessful String,
passes_away_progressivePasses String,
passes_away_progressivePassesSuccessful String,
passes_away_shortMediumPasses String,
passes_away_shortMediumPassesSuccessful String,
passes_away_shotAssists String,
passes_away_smartPasses String,
passes_away_smartPassesSuccessful String,
passes_away_throughPasses String,
passes_away_throughPassesSuccessful String,
passes_away_verticalPasses String,
passes_away_verticalPassesSuccessful String,
passes_home_assists string,
passes_home_avgPassLength String,
passes_home_avgPassToFinalThirdLength String,
passes_home_backPasses String,
passes_home_backPassesSuccessful String,
passes_home_crossesBlocked String,
passes_home_crossesFromLeftFlank String,
passes_home_crossesFromLeftFlankSuccessful String,
passes_home_crossesFromRightFlank String,
passes_home_crossesFromRightFlankSuccessful String,
passes_home_crossesHigh String,
passes_home_crossesLow String,
passes_home_crossesSuccessful String,
passes_home_crossesTotal String,
passes_home_deepCompletedPasses String,
passes_home_deepCompletedPassesSuccessful String,
passes_home_forwardPasses String,
passes_home_forwardPassesSuccessful String,
passes_home_keyPasses String,
passes_home_keyPassesSuccessful String,
passes_home_lateralPasses String,
passes_home_lateralPassesSuccessful String,
passes_home_longPasses String,
passes_home_longPassesSuccessful String,
passes_home_matchTempo String,
passes_home_passToFinalThirds String,
passes_home_passToFinalThirdsSuccessful String,
passes_home_passToPenaltyAreas String,
passes_home_passToPenaltyAreasSuccessful String,
passes_home_passes String,
passes_home_passesSuccessful String,
passes_home_progressivePasses String,
passes_home_progressivePassesSuccessful String,
passes_home_shortMediumPasses String,
passes_home_shortMediumPassesSuccessful String,
passes_home_shotAssists String,
passes_home_smartPasses String,
passes_home_smartPassesSuccessful String,
passes_home_throughPasses String,
passes_home_throughPassesSuccessful String,
passes_home_verticalPasses String,
passes_home_verticalPassesSuccessful String,
possession_away_avgPossessionDuration String,
`possession_away_minutesOfPossession1-15` String,
`possession_away_minutesOfPossession106-120` String,
`possession_away_minutesOfPossession16-30` String,
`possession_away_minutesOfPossession31-45` String,
`possession_away_minutesOfPossession46-60` String,
`possession_away_minutesOfPossession61-75` String,
`possession_away_minutesOfPossession76-90` String,
`possession_away_minutesOfPossession91-105` String,
`possession_away_possession1-15` String,
`possession_away_possession106-120` String,
`possession_away_possession16-30` String,
`possession_away_possession31-45` String,
`possession_away_possession46-60` String,
`possession_away_possession61-75` String,
`possession_away_possession76-90` String,
`possession_away_possession91-105` String,
possession_away_possessionNumber String,
possession_away_possessionPercent String,
possession_away_purePossessionTime String,
possession_away_reachingOpponentBox String,
possession_away_reachingOpponentHalf String,
possession_home_avgPossessionDuration String,
`possession_home_minutesOfPossession1-15` String,
`possession_home_minutesOfPossession106-120` String,
`possession_home_minutesOfPossession16-30` String,
`possession_home_minutesOfPossession31-45` String,
`possession_home_minutesOfPossession46-60` String,
`possession_home_minutesOfPossession61-75` String,
`possession_home_minutesOfPossession76-90` String,
`possession_home_minutesOfPossession91-105` String,
`possession_home_possession1-15` String,
`possession_home_possession106-120` String,
`possession_home_possession16-30` String,
`possession_home_possession31-45` String,
`possession_home_possession46-60` String,
`possession_home_possession61-75` String,
`possession_home_possession76-90` String,
`possession_home_possession91-105` String,
possession_home_possessionNumber String,
possession_home_possessionPercent String,
possession_home_purePossessionTime String,
possession_home_reachingOpponentBox String,
possession_home_reachingOpponentHalf String,
teams_away_teamId String, 
teams_home_teamId String,
transitions_away_lossesHigh String,
transitions_away_lossesLow String,
transitions_away_lossesMedium String,
transitions_away_lossesTotal String,
transitions_away_opponentHalfRecoveries String,
transitions_away_ownHalfLosses String,
transitions_away_recoveriesHigh String,
transitions_away_recoveriesLow String,
transitions_away_recoveriesMedium String,
transitions_away_recoveriesTotal String,
transitions_home_lossesHigh String,
transitions_home_lossesLow String,
transitions_home_lossesMedium String,
transitions_home_lossesTotal String,
transitions_home_opponentHalfRecoveries String,
transitions_home_ownHalfLosses String,
transitions_home_recoveriesHigh String,
transitions_home_recoveriesLow String,
transitions_home_recoveriesMedium String,
transitions_home_recoveriesTotal String,
META_UNVALIDATED Boolean,
META_EXTRACTION_DATE Timestamp,
META_LOAD_DATE Timestamp )

PARTITIONED BY (
META_EXTRACTION_DATE_YEAR Smallint,
META_EXTRACTION_DATE_MONTH Smallint,
META_EXTRACTION_DATE_DAY Smallint)
                                      
LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME,'wyscout_matches_advanced_stats',STANDARIZED_RAW_CONTAINER))

