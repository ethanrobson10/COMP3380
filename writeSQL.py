# Luc stinks

import pandas as pd

FIRST_SEASON = "2018"

SQL_CREATE_TABLES = """

USE cs3380;

SET NOCOUNT ON;

DROP TABLE IF EXISTS officiatedBy;
DROP TABLE IF EXISTS playsIn;
DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS venues;
DROP TABLE IF EXISTS teams; 
DROP TABLE IF EXISTS players;
DROP TABLE IF EXISTS officials;

CREATE TABLE teams (
  teamID INT PRIMARY KEY,
  city varchar(30) NOT NULL,
  teamName varchar(30) NOT NULL
);

CREATE TABLE venues (
  venueID INT PRIMARY KEY,
  venueName varchar(50) NOT NULL
  -- timezone varchar(30) NOT NULL
);


CREATE TABLE games (
  gameID INT PRIMARY KEY,
  type varchar(10) NOT NULL,
  dateTime DATETIME NOT NULL,
  outcome varchar(30) NOT NULL,
  homeTeamID INT,
  awayTeamID INT,
  venueID INT,

  FOREIGN KEY (venueID) REFERENCES venues (venueID)
    ON DELETE NO ACTION,
  FOREIGN KEY (homeTeamID) REFERENCES teams (teamID)
    ON DELETE NO ACTION,
  FOREIGN KEY (awayTeamID) REFERENCES teams (teamID)
    ON DELETE NO ACTION
);

CREATE TABLE players (
  playerID INT PRIMARY KEY,
  firstName varchar(30) NOT NULL,
  lastName varchar(30) NOT NULL,
  nationality varchar(30) NOT NULL,
  birthDate DATE NOT NULL,
  height varchar(30) NOT NULL,
  weight INT NOT NULL,
  playerType varchar(30)
);

CREATE TABLE playsIn (
  gameID INT,
  playerID INT,
  plusMinus INT,
  savePercentage FLOAT,

  FOREIGN KEY (gameID) REFERENCES games (gameID)
    ON DELETE NO ACTION,
  FOREIGN KEY (playerID) REFERENCES players (playerID)
    ON DELETE NO ACTION,
  PRIMARY KEY (gameID, playerID)
);

CREATE TABLE officials (
  officialID INT PRIMARY KEY,
  officialName varchar(30)
);

CREATE TABLE officiatedBy (
  gameID INT,
  officialID INT,
  officialType varchar(30),

  FOREIGN KEY (gameID) REFERENCES games (gameID)
    ON DELETE NO ACTION,
  FOREIGN KEY (officialID) REFERENCES officials (officialID)
    ON DELETE NO ACTION,
  PRIMARY KEY (gameID, officialID)
);

"""

# table I have so far
playsTable = """

CREATE TABLE plays(
    playID INT, -- PK
    playerID INT, -- FK
    gameID INT, -- FK
    shiftID INT, -- FK
    perdiodNum INT,
    periodType varchar(15),
    periodTime INT, 
    event varchar(15),
         CHECK (event IN ('Shot', 'Goal', 'Hit')),
    secondaryType varchar(30)

    FOREIGN KEY (playerID) REFERENCES players(playerID)
        ON DELETE NO ACTION,
    FOREIGN KEY (gameID) REFERENCES games (gameID)
        ON DELETE NO ACTION,
    FOREIGN KEY (shiftID) REFERENCES shifts(shiftID)
        ON DELETE NO ACTION
);
   
"""

# returns pandas df
def create_teams_df():

  teams = pd.read_csv("../data/team_info.csv")

  teams.rename(columns={"shortName":"city", "team_id":"teamID"}, inplace=True)
  teams = teams[["teamID", "city", "teamName"]]

  teams.loc[teams["city"].isin(["NY Rangers", "NY Islanders"]), "city"] = "New York"

  return teams

def create_venues_df():
  # Filter out venues for only those in timeframe we use?
  games = pd.read_csv("../data/game.csv")

  venues = games[["venue"]].drop_duplicates()

  venues.rename(columns={"venue":"venueName"}, inplace = True)

  venues["venueID"] = range(1, len(venues) + 1)

  fix_apostrophes(venues)
  # venues["venueName"] = venues["venueName"].str.replace("'", "''") # two apostrophes is one in a SQL string 
  venues["venueName"] = venues["venueName"].str.replace("  ", "")
  
  # 177 unique venues and timezones
  # 116 unque venue names
  # should we just REMOVE timezones?

  dict_venue_mapper = dict(zip(venues["venueName"], venues["venueID"]))

  return venues, dict_venue_mapper


def create_games_df(venueID_mapper):
  games = pd.read_csv("../data/game.csv")

  games["venueID"] = games["venue"].map(venueID_mapper)
  games.rename(columns={"game_id":"gameID", "date_time_GMT":"dateTime", "home_team_id":"homeTeamID", "away_team_id":"awayTeamID"}, inplace=True)
  games = games[["gameID", "type", "dateTime", "outcome", "homeTeamID", "awayTeamID", "venueID"]]

  games["dateTime"] = games["dateTime"].str.replace("T", " ")
  games["dateTime"] = games["dateTime"].str.replace("Z", "")

  # filter games only keeping seasons after FIRST SEASON
  games = games.loc[(games["dateTime"].str[:4] >= FIRST_SEASON) & (games["dateTime"].str[5:7] >= "09")]

  # cuts dataframe in half (each row is duplicated?)
  games = games.drop_duplicates()

  return games

def create_shifts_df():
  shifts = pd.read_csv("../data/game_shifts.csv")

  # Rename columns for consistency
  shifts = shifts.rename(columns={"game_id": "gameID", "player_id": "playerID", "shift_start": "shiftStart", "shift_end": "shiftEnd"})
  shifts = shifts[["gameID", "playerID", "shiftStart", "shiftEnd"]]

  # Assign unique IDs for each shift
  shifts["shiftID"] = range(1, len(shifts) + 1)
  
  # Define the duration of each period in seconds
  PERIOD_DURATION = 1200

  # Calculate period number and period-relative shift start
  shifts["periodNumber"] = shifts["shiftStart"] // PERIOD_DURATION + 1
  shifts["adjustedShiftStart"] = shifts["shiftStart"] % PERIOD_DURATION
  shifts["adjustedShiftEnd"] = shifts["shiftEnd"] % PERIOD_DURATION

  # ***Dont need dictionary anymore***
  # Create the dictionary mapper with (gameID, playerID, periodNumber, adjustedShiftStart) as the key
  # dict_shift_mapper = dict(zip(
  #     zip(shifts["gameID"], shifts["playerID"], shifts["periodNumber"], shifts["adjustedShiftStart"], shifts["adjustedShiftEnd"]),
  #     shifts["shiftID"]
  # ))
  return shifts #, dict_shift_mapper

def create_plays_df(shifts_df, valid_game_ids):

  # filter games in timeframe
  plays_noPlayerID = pd.read_csv("../data/game_plays.csv")
  plays_noPlayerID = plays_noPlayerID.loc[plays_noPlayerID["gameID"].isin(valid_game_ids)]

  # filter 1) only important types of plays 2) games in timeframe,
  plays_playerID = pd.read_csv("../data/game_plays_players.csv") 
  values = ["Hitter", "Scorer", "Shooter"]
  plays_playerID = plays_playerID.loc[plays_playerID["gameID"].isin(values)]
  plays_playerID = plays_playerID.loc[plays_playerID["gameID"].isin(valid_game_ids)]

  # rename the columns and remove the ones we dont want 
  plays_noPlayerID.rename(columns={"play_id": "playID", "game_id": "gameID", "period": "periodNum"}, inplace=True)
  plays_noPlayerID = plays_noPlayerID[["playID", "gameID", "shiftID", "periodNum", "periodType", "periodTime"
                                       "event", "secondaryType"]] 
  
  plays_playerID.rename(columns={"play_id": "playID", "player_id": "playerID"}, inplace=True)
  plays_playerID = plays_playerID[["playID", "playerID"]] 

  # join the two csv files on the playID 
  plays = pd.merge([plays_noPlayerID, plays_playerID], how="inner", on="playID")
  plays = plays[["playID", "playerID", "gameID", "shiftID", "periodNum", 
                 "periodType", "periodTime" "event", "secondaryType"]]
  
  plays_shifts = pd.merge([plays, shifts_df], how="inner")

  plays = plays_shifts.loc[plays_shifts["periodTime"].between(plays_shifts["adjustedShiftStart"], plays_shifts["adjustedShiftEnd"])]
  # plays = plays_shifts.loc[plays_shifts[‘playTime’].between(plays_shifts[‘shiftStart’], plays_shifts[‘shiftEnd’])]

  return plays

def create_player_df():
  players = pd.read_csv("../data/player_info.csv")

  players.rename(columns={"player_id":"playerID"}, inplace=True)

  players = players[["playerID", "firstName", "lastName", "nationality", "birthDate", "height", "weight", "primaryPosition"]]
  # players["height"] = players["height"].str.replace("'", "''")
  fix_apostrophes(players)

  # fix missing values
  players.fillna({"weight":players["weight"].mean(), "height":"6'' 1\"", "nationality": "CAN"}, inplace=True)

  # 0 players removed now -- removes 8 players with atleast one null values
  players = players[players.notnull().all(axis=1)]

  players["playerType"] = players["primaryPosition"].apply(lambda x: "Goalie" if x == "G" else "Skater")
  
  players = players.drop(columns = ["primaryPosition"])

  return players

  
def create_playsIn_df(valid_game_ids):
  skater_game = pd.read_csv("../data/game_skater_stats.csv")
  goalie_game = pd.read_csv("../data/game_goalie_stats.csv")


  skater_game.rename(columns={"game_id":"gameID", "player_id":"playerID"}, inplace=True)
  skater_game = skater_game[["gameID", "playerID", "plusMinus"]]

  goalie_game.rename(columns={"game_id":"gameID", "player_id":"playerID"}, inplace=True)
  goalie_game = goalie_game[["gameID", "playerID", "savePercentage"]]

  playsIn = pd.merge(skater_game, goalie_game, how='outer')

  # filter df for only games in our time frame
  playsIn = playsIn.loc[playsIn["gameID"].isin(valid_game_ids)]

  # same - cuts data frame in half
  playsIn = playsIn.drop_duplicates()

  return playsIn

def create_playsOn_df(games):
  skater_game = pd.read_csv("../data/game_skater_stats.csv")
  goalie_game = pd.read_csv("../data/game_goalie_stats.csv")

  skater_game = skater_game[["game_id", "player_id", "team_id"]]
  goalie_game = goalie_game[["game_id", "player_id", "team_id"]]

  players_game = pd.concat([skater_game, goalie_game], ignore_index=True)

  players_game.rename(columns={"game_id":"gameID", "player_id":"playerID", "team_id":"teamID"}, inplace=True)

  players_game = pd.merge(players_game, games, how="inner")
  
  playsOn = pd.DataFrame(columns={"teamID", "playerID", "startDate", "endDate"})

  players = players_game["playerID"].drop_duplicates()

  print(len(players))

  for player_id in players:
    
    this_players_games = players_game.loc[players_game["playerID"] == player_id]
    this_players_games = this_players_games.sort_values(by="dateTime")

    curr_team = this_players_games.iloc[0]["teamID"]
    prev_team = -1

    new_row = pd.DataFrame({"teamID":[curr_team], "playerID":[player_id], "startDate":[f"{FIRST_SEASON}-09-00"], "endDate":[pd.NA]})
    playsOn = pd.concat([playsOn, new_row], ignore_index=True)
    # playsOn.loc[len(playsOn)] = [curr_team, player_id, f"{FIRST_SEASON}-09-00", pd.NA]

    for row in this_players_games.iterrows():
      if(row[1]["teamID"] != curr_team):
        curr_team = row[1]["teamID"]
        new_team_date = row[1]["dateTime"][:10]
        playsOn.loc[len(playsOn) - 1, "endDate"] = new_team_date

        new_row = pd.DataFrame({"teamID":[curr_team], "playerID":[player_id], "startDate":new_team_date, "endDate":[pd.NA]})
        playsOn = pd.concat([playsOn, new_row], ignore_index=True)

        # print(playsOn.iloc[len(playsOn) - 1])
        # print(playsOn.iloc[len(playsOn) - 2])
      

      
      
  return playsOn

  
def create_officials_df():
  officials = pd.read_csv("../data/game_officials.csv")

  officials.rename(columns={"official_name":"officialName"}, inplace=True)
  officials = officials[["officialName"]].drop_duplicates()

  fix_apostrophes(officials)

  officials["officialID"] = range(1, len(officials) + 1)

  return officials

# note: a couple games of 5 and 6 refs
def create_officiatedBy_df(officials_df, valid_game_ids):
  officials_games = pd.read_csv("../data/game_officials.csv")

  officials_games.rename(columns={"game_id": "gameID", "official_name":"officialName", "official_type": "officialType"}, inplace=True)
  officials_games = officials_games.drop_duplicates()

  fix_apostrophes(officials_games)

  # filter df for only games in our time frame
  officials_games = officials_games.loc[officials_games["gameID"].isin(valid_game_ids)]

  officials_games = pd.merge(officials_games, officials_df, on="officialName")
  officials_games = officials_games[["gameID", "officialID", "officialType"]].drop_duplicates()

  return officials_games


def create_inserts(df, table_name):

  col_names = ", ".join(df.columns)
  insert_string = f"INSERT INTO {table_name} ({col_names}) VALUES "

  individual_inserts = []

  for row in df.iterrows():
    values = []
    for val in row[1]:
      values.append(f"'{val}'" if isinstance(val, str) else str(val))

    values = ['NULL' if x == 'nan' else x for x in values] # replace missing values 'nan' with NULL

    values = ", ".join(values) # separate each value with comma and space
    individual_inserts.append(f"{insert_string}({values});\n")

  insert_string = "\n\n" + "".join(individual_inserts) #full insert statement for given table

  return insert_string

def create_bulk_insert(df, table_name):
  df.to_csv(f"{table_name}.csv", index = False)
  bulk_insert = f"\nBULK INSERT {table_name}\nFROM '{table_name}.csv'\nWITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', FIRSTROW = 2);"

  return bulk_insert


def fix_apostrophes(df):
  for i in range(df.shape[1]):
    if((df.iloc[:,i]).dtype == "object"):

      # print((df.iloc[:,i]).dtype)
      df.iloc[:,i] = df.iloc[:,i].str.replace("'", "''")


def main():
  all_inserts = ""

  teams = create_teams_df()
  all_inserts += create_inserts(teams, "teams")

  venues, venueID_mapper = create_venues_df()
  all_inserts += create_inserts(venues, "venues")

  games = create_games_df(venueID_mapper)
  all_inserts += create_inserts(games, "games")

  players = create_player_df()
  all_inserts += create_inserts(players, "players")

  playsIn = create_playsIn_df(games["gameID"])
  all_inserts += create_inserts(playsIn, "playsIn")

  officials = create_officials_df()
  all_inserts += create_inserts(officials, "officials")

  officiatedBy = create_officiatedBy_df(officials, games["gameID"])
  all_inserts += create_inserts(officiatedBy, "officiatedBy")

  with open('populate.sql', 'w') as file:
    file.write(SQL_CREATE_TABLES)
    file.write(all_inserts)

  print("\nSQL file created successfully")

main()


# ---- Says dont have permission to do this
# BULK INSERT MyTable
# FROM 'C:\Path\To\Your\File.csv'
# WITH (
#     FIELDTERMINATOR = ',', 
#     ROWTERMINATOR = '\n',
#     FIRSTROW = 2
# );