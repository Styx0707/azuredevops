# Databricks notebook source
# MAGIC %run "../Framework/Configuration Management"

# COMMAND ----------

BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
STAGING_RAW_CONTAINER=get_parameter("GLOBAL","STAGING_RAW_CONTAINER")

print(BRONZE_CATALOG_NAME)
print(STAGING_RAW_CONTAINER)

# COMMAND ----------

data_provider = 'wyscout'
data_source_name = 'matches_formations'
table_name = data_provider+'_'+data_source_name
display(table_name)

# COMMAND ----------

############ Create Areas Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(areas_alpha2code String, 
areas_alpha3code String, 
areas_id String,
areas_name String )
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create Coaches Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
passportArea_name String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create referees Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(birthDate String, 
firstName String ,
gender String, 
lastName String,
middleName String,
shortName String,
status String,
wyId String,
alpha2code String,
alpha3code String,
id String,
name String,
passportArea_alpha2code String,
passportArea_alpha3code String,
passportArea_id String,
passportArea_name String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create Rounds Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String, 
endDate String ,
name String, 
seasonId String,
startDate String,
type String,
wyId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create competitions_areas Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
competitions_area_name String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create competitions_matches Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String, 
date String ,
dateutc String, 
gameweek String,
label String,
matchId String,
roundId String,
seasonId String,
status String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create competitions_teams Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
children_wyId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create competitions_players Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(page_count String, 
page_current String ,
page_size String, 
total_items String,
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
appliedFilters_advsearch String,
appliedFilters_fetch String,
appliedFilters_filter String,
appliedFilters_limit String,
appliedFilters_page String,
appliedFilters_search String,
appliedFilters_sort String,
availableFilters_advsearches String,
availableFilters_fetches String,
availableFilters_filters String,
availableFilters_searches String,
availableFilters_sorts String,
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
players_role_name String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create competitions_seasons Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String, 
seasonId String ,
season_active String, 
season_competitionId String,
season_endDate String,
season_name String,
season_startDate String,
season_wyId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create competitions Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
area_name String )
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create teams Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
children_wyId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create teams_careers Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
career_teamId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create teams_fixtures Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(matches String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create teams_matches Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String, 
date String ,
dateutc String,
gameweek String,
label String,
matchId String,
roundId String,
seasonId String,
status String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(active String, 
competitionId String ,
endDate String,
name String,
startDate String,
wyId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons_assistmen Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
( competitionId String ,
seasonId String,
assists String,
playerId String,
teamId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons_careers Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
( rounds_roundId String ,
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
rounds_groups_teams_teamId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons_fixtures Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String ,
seasonId String,
matchId String,
goals_minute String,
goals_period String,
goals_playerId String,
goals_teamId String,
goals_type String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons_matches Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String ,
date String,
dateutc String,
gameweek String,
label String,
matchId String,
roundId String,
seasonId String,
status String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons_scorers Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String ,
seasonId String,
goals String,
playerId String,
teamId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons_teams Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(category String ,
city String,
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
children_wyId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create seasons_standings Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String ,
seasonId String,
groupName String,
teamId String,
totalDraws String,
totalGoalsAgainst String,
totalGoalsFor String,
totalLosses String,
totalPlayed String,
totalPoints String,
totalWins String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create players_matches Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String ,
date String,
dateutc String,
gameweek String,
label String,
matchId String,
roundId String,
seasonId String,
status String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create players_contractinfo Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(agencies String, 
contractExpiration String, 
playerId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create players_fixtures Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(matches String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create players_careers Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
career_yellowCard String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create matches Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
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
refereeId String,
role String,
teamsData_31148_coachId String,
teamsData_31148_formation String,
teamsData_31148_hasFormation String,
teamsData_31148_score String,
teamsData_31148_scoreET String,
teamsData_31148_scoreHT String,
teamsData_31148_scoreP String,
teamsData_31148_side String,
teamsData_31148_teamId String,
teamsData_5085_coachId String,
teamsData_5085_formation String,
teamsData_5085_hasFormation String,
teamsData_5085_score String,
teamsData_5085_scoreET String,
teamsData_5085_scoreHT String,
teamsData_5085_scoreP String,
teamsData_5085_side String,
teamsData_5085_teamId String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create matches_formations Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(`1H_0_4-3-3_endSec` String ,
`1H_0_4-3-3_id` String,
`1H_0_4-3-3_matchPeriod` String,
`1H_0_4-3-3_scheme` String,
`1H_0_4-3-3_startSec` String,
`2H_2634_4-1-3-1_endSec` String,
`2H_2634_4-1-3-1_id` String,
`2H_2634_4-1-3-1_matchPeriod` String,
`2H_2634_4-1-3-1_scheme` String,
`2H_2634_4-1-3-1_startSec` String,
`2H_927_4-4-1_endSec` String,
`2H_927_4-4-1_id` String,
`2H_927_4-4-1_matchPeriod` String,
`2H_927_4-4-1_scheme` String,
`2H_927_4-4-1_startSec` String,
`5083_1H_0_4-2-3-1_endSec` String,
`5083_1H_0_4-2-3-1_id` String,
`5083_1H_0_4-2-3-1_matchPeriod` String,
`5083_1H_0_4-2-3-1_scheme` String,
`5083_1H_0_4-2-3-1_startSec` String,
`5083_2H_1799_4-4-2_endSec` String,
`5083_2H_1799_4-4-2_id` String,
`5083_2H_1799_4-4-2_matchPeriod` String,
`5083_2H_1799_4-4-2_scheme` String,
`5083_2H_1799_4-4-2_startSec` String,
`5083_2H_2656_4-4-2_endSec` String,
`5083_2H_2656_4-4-2_id` String,
`5083_2H_2656_4-4-2_matchPeriod` String,
`5083_2H_2656_4-4-2_scheme` String,
`5083_2H_2656_4-4-2_startSec` String,
`5083_2H_2856_4-4-2_endSec` String,
`5083_2H_2856_4-4-2_id` String,
`5083_2H_2856_4-4-2_matchPeriod` String,
`5083_2H_2856_4-4-2_scheme` String,
`5083_2H_2856_4-4-2_startSec` String,
`1H_0_4-3-3_players_15136_playerId` String,
`1H_0_4-3-3_players_15136_position` String,
`1H_0_4-3-3_players_201_playerId` String,
`1H_0_4-3-3_players_201_position` String,
`1H_0_4-3-3_players_26088_playerId` String,
`1H_0_4-3-3_players_26088_position` String,
`1H_0_4-3-3_players_26869_playerId` String,
`1H_0_4-3-3_players_26869_position` String,
`1H_0_4-3-3_players_26944_playerId` String,
`1H_0_4-3-3_players_26944_position` String,
`1H_0_4-3-3_players_37824_playerId` String,
`1H_0_4-3-3_players_37824_position` String,
`1H_0_4-3-3_players_37826_playerId` String,
`1H_0_4-3-3_players_37826_position` String,
`1H_0_4-3-3_players_37834_playerId` String,
`1H_0_4-3-3_players_37834_position` String,
`1H_0_4-3-3_players_37928_playerId` String,
`1H_0_4-3-3_players_37928_position` String,
`1H_0_4-3-3_players_38709_playerId` String,
`1H_0_4-3-3_players_38709_position` String,
`1H_0_4-3-3_players_91381_playerId` String,
`1H_0_4-3-3_players_91381_position` String,
`2H_2634_4-1-3-1_players_116267_playerId` String,
`2H_2634_4-1-3-1_players_116267_position` String,
`2H_2634_4-1-3-1_players_134710_playerId` String,
`2H_2634_4-1-3-1_players_134710_position` String,
`2H_2634_4-1-3-1_players_15136_playerId` String,
`2H_2634_4-1-3-1_players_15136_position` String,
`2H_2634_4-1-3-1_players_26869_playerId` String,
`2H_2634_4-1-3-1_players_26869_position` String,
`2H_2634_4-1-3-1_players_26944_playerId` String,
`2H_2634_4-1-3-1_players_26944_position` String,
`2H_2634_4-1-3-1_players_37824_playerId` String,
`2H_2634_4-1-3-1_players_37824_position` String,
`2H_2634_4-1-3-1_players_37826_playerId` String,
`2H_2634_4-1-3-1_players_37826_position` String,
`2H_2634_4-1-3-1_players_37834_playerId` String,
`2H_2634_4-1-3-1_players_37834_position` String,
`2H_2634_4-1-3-1_players_37928_playerId` String,
`2H_2634_4-1-3-1_players_37928_position` String,
`2H_2634_4-1-3-1_players_38035_playerId` String,
`2H_2634_4-1-3-1_players_38035_position` String,
`2H_927_4-4-1_players_134710_playerId` String,
`2H_927_4-4-1_players_134710_position` String,
`2H_927_4-4-1_players_15136_playerId` String,
`2H_927_4-4-1_players_15136_position` String,
`2H_927_4-4-1_players_201_playerId` String,
`2H_927_4-4-1_players_201_position` String,
`2H_927_4-4-1_players_26869_playerId` String,
`2H_927_4-4-1_players_26869_position` String,
`2H_927_4-4-1_players_26944_playerId` String,
`2H_927_4-4-1_players_26944_position` String,
`2H_927_4-4-1_players_37824_playerId` String,
`2H_927_4-4-1_players_37824_position` String,
`2H_927_4-4-1_players_37826_playerId` String,
`2H_927_4-4-1_players_37826_position` String,
`2H_927_4-4-1_players_37834_playerId` String,
`2H_927_4-4-1_players_37834_position` String,
`2H_927_4-4-1_players_37928_playerId` String,
`2H_927_4-4-1_players_37928_position` String,
`2H_927_4-4-1_players_38709_playerId` String,
`2H_927_4-4-1_players_38709_position` String,
`5083_1H_0_4-2-3-1_players_15004_playerId` String,
`5083_1H_0_4-2-3-1_players_15004_position` String,
`5083_1H_0_4-2-3-1_players_223050_playerId` String,
`5083_1H_0_4-2-3-1_players_223050_position` String,
`5083_1H_0_4-2-3-1_players_248632_playerId` String,
`5083_1H_0_4-2-3-1_players_248632_position` String,
`5083_1H_0_4-2-3-1_players_25540_playerId` String,
`5083_1H_0_4-2-3-1_players_25540_position` String,
`5083_1H_0_4-2-3-1_players_26512_playerId` String,
`5083_1H_0_4-2-3-1_players_26512_position` String,
`5083_1H_0_4-2-3-1_players_267057_playerId` String,
`5083_1H_0_4-2-3-1_players_267057_position` String,
`5083_1H_0_4-2-3-1_players_3332_playerId` String,
`5083_1H_0_4-2-3-1_players_3332_position` String,
`5083_1H_0_4-2-3-1_players_56631_playerId` String,
`5083_1H_0_4-2-3-1_players_56631_position` String,
`5083_1H_0_4-2-3-1_players_91253_playerId` String,
`5083_1H_0_4-2-3-1_players_91253_position` String,
`5083_1H_0_4-2-3-1_players_92901_playerId` String,
`5083_1H_0_4-2-3-1_players_92901_position` String,
`5083_1H_0_4-2-3-1_players_99574_playerId` String,
`5083_1H_0_4-2-3-1_players_99574_position` String,
`5083_2H_1799_4-4-2_players_15004_playerId` String,
`5083_2H_1799_4-4-2_players_15004_position` String,
`5083_2H_1799_4-4-2_players_223050_playerId` String,
`5083_2H_1799_4-4-2_players_223050_position` String,
`5083_2H_1799_4-4-2_players_248632_playerId` String,
`5083_2H_1799_4-4-2_players_248632_position` String,
`5083_2H_1799_4-4-2_players_26512_playerId` String,
`5083_2H_1799_4-4-2_players_26512_position` String,
`5083_2H_1799_4-4-2_players_267057_playerId` String,
`5083_2H_1799_4-4-2_players_267057_position` String,
`5083_2H_1799_4-4-2_players_3332_playerId` String,
`5083_2H_1799_4-4-2_players_3332_position` String,
`5083_2H_1799_4-4-2_players_38274_playerId` String,
`5083_2H_1799_4-4-2_players_38274_position` String,
`5083_2H_1799_4-4-2_players_56631_playerId` String,
`5083_2H_1799_4-4-2_players_56631_position` String,
`5083_2H_1799_4-4-2_players_91253_playerId` String,
`5083_2H_1799_4-4-2_players_91253_position` String,
`5083_2H_1799_4-4-2_players_92901_playerId` String,
`5083_2H_1799_4-4-2_players_92901_position` String,
`5083_2H_1799_4-4-2_players_99574_playerId` String,
`5083_2H_1799_4-4-2_players_99574_position` String,
`5083_2H_2656_4-4-2_players_15004_playerId` String,
`5083_2H_2656_4-4-2_players_15004_position` String,
`5083_2H_2656_4-4-2_players_248632_playerId` String,
`5083_2H_2656_4-4-2_players_248632_position` String,
`5083_2H_2656_4-4-2_players_26512_playerId` String,
`5083_2H_2656_4-4-2_players_26512_position` String,
`5083_2H_2656_4-4-2_players_267057_playerId` String,
`5083_2H_2656_4-4-2_players_267057_position` String,
`5083_2H_2656_4-4-2_players_3332_playerId` String,
`5083_2H_2656_4-4-2_players_3332_position` String,
`5083_2H_2656_4-4-2_players_38073_playerId` String,
`5083_2H_2656_4-4-2_players_38073_position` String,
`5083_2H_2656_4-4-2_players_38274_playerId` String,
`5083_2H_2656_4-4-2_players_38274_position` String,
`5083_2H_2656_4-4-2_players_56631_playerId` String,
`5083_2H_2656_4-4-2_players_56631_position` String,
`5083_2H_2656_4-4-2_players_91253_playerId` String,
`5083_2H_2656_4-4-2_players_91253_position` String,
`5083_2H_2656_4-4-2_players_92901_playerId` String,
`5083_2H_2656_4-4-2_players_92901_position` String,
`5083_2H_2656_4-4-2_players_99574_playerId` String,
`5083_2H_2656_4-4-2_players_99574_position` String,
`5083_2H_2856_4-4-2_players_102085_playerId` String,
`5083_2H_2856_4-4-2_players_102085_position` String,
`5083_2H_2856_4-4-2_players_248632_playerId` String,
`5083_2H_2856_4-4-2_players_248632_position` String,
`5083_2H_2856_4-4-2_players_26512_playerId` String,
`5083_2H_2856_4-4-2_players_26512_position` String,
`5083_2H_2856_4-4-2_players_267057_playerId` String,
`5083_2H_2856_4-4-2_players_267057_position` String,
`5083_2H_2856_4-4-2_players_3332_playerId` String,
`5083_2H_2856_4-4-2_players_3332_position` String,
`5083_2H_2856_4-4-2_players_38073_playerId` String,
`5083_2H_2856_4-4-2_players_38073_position` String,
`5083_2H_2856_4-4-2_players_38274_playerId` String,
`5083_2H_2856_4-4-2_players_38274_position` String,
`5083_2H_2856_4-4-2_players_56631_playerId` String,
`5083_2H_2856_4-4-2_players_56631_position` String,
`5083_2H_2856_4-4-2_players_91253_playerId` String,
`5083_2H_2856_4-4-2_players_91253_position` String,
`5083_2H_2856_4-4-2_players_92901_playerId` String,
`5083_2H_2856_4-4-2_players_92901_position` String,
`5083_2H_2856_4-4-2_players_99574_playerId` String,
`5083_2H_2856_4-4-2_players_99574_position`String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create videos_qualities Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(matchId String ,
isAvailable String,
hd_isAvailable String,
lq_isAvailable String,
sd_isAvailable String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create players_advancedstats Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(competitionId String ,
playerId String,
positions String,
roundId String,
seasonId String,
accelerations String,
aerialDuels String,
assists String,
attackingActions String,
backPasses String,
ballLosses String,
ballRecoveries String,
clearances String,
corners String,
counterpressingRecoveries String,
crosses String,
dangerousOpponentHalfRecoveries String,
dangerousOwnHalfLosses String,
defensiveActions String,
defensiveDuels String,
defensiveDuelsWon String,
directFreeKicks String,
directFreeKicksOnTarget String,
directRedCards String,
dribbleDistanceFromOpponentGoal String,
dribbles String,
dribblesAgainst String,
dribblesAgainstWon String,
duels String,
duelsWon String,
fieldAerialDuels String,
fieldAerialDuelsWon String,
forwardPasses String,
fouls String,
foulsSuffered String,
freeKicks String,
freeKicksOnTarget String,
gkAerialDuels String,
gkAerialDuelsWon String,
gkConcededGoals String,
gkExits String,
gkSaves String,
gkShotsAgainst String,
gkSuccessfulExits String,
goalKicks String,
goalKicksLong String,
goalKicksShort String,
goals String,
headShots String,
interceptions String,
keyPasses String,
lateralPasses String,
linkupPlays String,
longPassLength String,
longPasses String,
looseBallDuels String,
looseBallDuelsWon String,
losses String,
missedBalls String,
newDefensiveDuelsWon String,
newDuelsWon String,
newOffensiveDuelsWon String,
newSuccessfulDribbles String,
offensiveDuels String,
offensiveDuelsWon String,
offsides String,
opponentHalfRecoveries String,
ownHalfLosses String,
passLength String,
passes String,
passesToFinalThird String,
penalties String,
progressivePasses String,
progressiveRun String,
receivedPass String,
redCards String,
secondAssists String,
shotAssists String,
shotOnTargetAssists String,
shots String,
shotsBlocked String,
shotsOnTarget String,
slidingTackles String,
smartPasses String,
successfulAttackingActions String,
successfulBackPasses String,
successfulCrosses String,
successfulDefensiveAction String,
successfulDribbles String,
successfulForwardPasses String,
successfulGoalKicks String,
successfulKeyPasses String,
successfulLateralPasses String,
successfulLinkupPlays String,
successfulLongPasses String,
successfulPasses String,
successfulPassesToFinalThird String,
successfulPenalties String,
successfulProgressivePasses String,
successfulSlidingTackles String,
successfulSmartPasses String,
successfulThroughPasses String,
successfulVerticalPasses String,
thirdAssists String,
throughPasses String,
touchInBox String,
verticalPasses String,
xgAssist String,
xgSave String,
xgShot String,
yellowCards String,
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
total_yellowCards String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------

############ Create matches_events Table #################

sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
(meta String ,
id String,
matchId String,
matchPeriod String,
matchTimestamp String,
minute String,
relatedEventId String,
second String,
videoTimestamp String,
aerialDuel_firstTouch String,
aerialDuel_height String,
aerialDuel_relatedDuelId String,
carry_progression String,
groundDuel_duelType String,
groundDuel_keptPossession String,
groundDuel_progressedWithBall String,
groundDuel_recoveredPossession String,
groundDuel_relatedDuelId String,
groundDuel_side String,
groundDuel_stoppedProgress String,
groundDuel_takeOn String,
infraction_redCard String,
infraction_type String,
infraction_yellowCard String,
location_x String,
location_y String,
opponentTeam_formation String,
opponentTeam_id String,
opponentTeam_name String,
pass_accurate String,
pass_angle String,
pass_height String,
pass_length String,
player_id String,
player_name String,
player_position String,
possession_duration String,
possession_eventIndex String,
possession_eventsNumber String,
possession_id String,
possession_types String,
shot_bodyPart String,
shot_goalZone String,
shot_goalkeeperActionId string,
shot_isGoal String,
shot_onTarget String,
shot_postShotXg String,
shot_xg String,
team_formation String,
team_id String,
team_name String,
type_primary String,
type_secondary String,
aerialDuel_opponent_height String,
aerialDuel_opponent_id String,
aerialDuel_opponent_name String,
aerialDuel_opponent_position String,
carry_endLocation_x String,
carry_endLocation_y String,
groundDuel_opponent_id String,
groundDuel_opponent_name String,
groundDuel_opponent_position String,
infraction_opponent_id String,
infraction_opponent_name String,
infraction_opponent_position String,
pass_endLocation_x String,
pass_endLocation_y String,
pass_recipient_id String,
pass_recipient_name String,
pass_recipient_position String,
possession_attack_flank String,
possession_attack_withGoal String,
possession_attack_withShot String,
possession_attack_withShotOnGoal String,
possession_attack_xg String,
possession_endLocation_x String,
possession_endLocation_y String,
possession_startLocation_x String,
possession_startLocation_y String,
possession_team_formation String,
possession_team_id String,
possession_team_name String,
shot_goalkeeper_id String,
shot_goalkeeper_name String)
USING DELTA 
""".format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
display(df1)

# COMMAND ----------


