import pandas as pd
import os

PATH = '../../data/'
team_info = PATH + "team_info.csv"
game = PATH + "game.csv"
player_info = PATH + "player_info.csv"
game_skater_stats = PATH + "game_skater_stats.csv"
game_goalie_stats = PATH + "game_goalie_stats.csv"
game_officials = PATH + "game_officials.csv"
game_shifts = PATH + "game_shifts.csv"
game_plays = PATH + "game_plays.csv"

FIRST_SEASON = "2012"

SQL_CREATE_TABLES = """

USE cs3380;

SET NOCOUNT ON;

DROP TABLE IF EXISTS assists;
DROP TABLE IF EXISTS plays;
DROP TABLE IF EXISTS shifts;
DROP TABLE IF EXISTS officiatedBy;
DROP TABLE IF EXISTS playsIn;
DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS venues;
DROP TABLE IF EXISTS playsOn;
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
  season varchar(15) NOT NULL,
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

CREATE TABLE playsOn (
  playerID INT,
  teamID INT,
  startDate DATE NOT NULL,
  endDate DATE,
  --check endDate after startdate?

  FOREIGN KEY (playerID) REFERENCES players (playerID)
    ON DELETE NO ACTION,
  FOREIGN KEY (teamID) REFERENCES teams (teamID)
    ON DELETE NO ACTION,
  PRIMARY KEY (playerID, teamID, startDate)
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

CREATE TABLE shifts (
  shiftID INT PRIMARY KEY,
  playerID INT,
  gameID INT,
  periodNumber INT NOT NULL,
  shiftStart INT NOT NULL,
  shiftEnd INT NOT NULL,

  FOREIGN KEY (playerID) REFERENCES players (playerID)
    ON DELETE NO ACTION,
  FOREIGN KEY (gameID) REFERENCES games (gameID)
    ON DELETE NO ACTION
);

CREATE TABLE plays(
    playID varchar(30) PRIMARY KEY, -- create our own playID to make integer
    playerID INT, -- FK
    gameID INT, -- FK
    shiftID INT, -- FK
    periodNumber INT,
    periodType varchar(15),
    periodTime INT, 
    playType varchar(15), 
         CHECK (playType IN ('Shot', 'Goal', 'Penalty')),
    secondaryType varchar(60), -- long descriptions for penalties
    goalieID INT,

    
    FOREIGN KEY (playerID) REFERENCES players(playerID)
        ON DELETE NO ACTION,
    FOREIGN KEY (gameID) REFERENCES games (gameID)
        ON DELETE NO ACTION,
    FOREIGN KEY (shiftID) REFERENCES shifts (shiftID)
        ON DELETE NO ACTION,
    FOREIGN KEY (goalieID) REFERENCES players(playerID)
        ON DELETE NO ACTION
);

CREATE TABLE assists (
  playID varchar(30), 
  playerID INT, 

  FOREIGN KEY (playerID) REFERENCES players (playerID)
    ON DELETE NO ACTION,
  FOREIGN KEY (playID) REFERENCES plays (playID)
    ON DELETE NO ACTION,
  PRIMARY KEY (playID, playerID)
);

"""

# returns pandas df
def create_teams_df():

  teams = pd.read_csv(team_info)

  teams.rename(columns={"shortName":"city", "team_id":"teamID"}, inplace=True)
  teams = teams[["teamID", "city", "teamName"]]

  teams.loc[teams["city"].isin(["NY Rangers", "NY Islanders"]), "city"] = "New York"

  convert_column_int(teams, ["teamID"])

  return teams

def create_venues_df():
  # Filter out venues for only those in timeframe we use?
  games = pd.read_csv(game)

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

  convert_column_int(venues, ["venueID"])

  return venues, dict_venue_mapper


def create_games_df(venueID_mapper):
  games = pd.read_csv(game)

  games["venueID"] = games["venue"].map(venueID_mapper)
  games.rename(columns={"game_id":"gameID", "date_time_GMT":"dateTime", "home_team_id":"homeTeamID", "away_team_id":"awayTeamID"}, inplace=True)
  games = games[["gameID", "type", "dateTime", "outcome", "season", "homeTeamID", "awayTeamID", "venueID"]]

  games["dateTime"] = games["dateTime"].str.replace("T", " ")
  games["dateTime"] = games["dateTime"].str.replace("Z", "")

  games["season"] = games["season"].astype(str)
  games["season"] = games["season"].str[:4] + "-" + games["season"].str[4:]

  # filter games only keeping seasons after FIRST SEASON
  games = games.loc[(games["dateTime"].str[:4] > FIRST_SEASON) | ((games["dateTime"].str[:4] == FIRST_SEASON) & (games["dateTime"].str[5:7] >= "09"))]

  # cuts dataframe in half (each row is duplicated?)
  games = games.drop_duplicates()

  # games["venueID"] = games["venueID"].astype(int)  

  games = games.loc[games["type"] != "A"]

  convert_column_int(games, ["gameID", "homeTeamID", "awayTeamID", "venueID"])

  return games


def create_player_df():
  players = pd.read_csv(player_info)

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

  convert_column_int(players, ["playerID", "weight"])

  return players

  
def create_playsIn_df(valid_game_ids):
  skater_game = pd.read_csv(game_skater_stats)
  goalie_game = pd.read_csv(game_goalie_stats)


  skater_game.rename(columns={"game_id":"gameID", "player_id":"playerID"}, inplace=True)
  skater_game = skater_game[["gameID", "playerID", "plusMinus"]]

  goalie_game.rename(columns={"game_id":"gameID", "player_id":"playerID"}, inplace=True)
  goalie_game = goalie_game[["gameID", "playerID", "savePercentage"]]

  playsIn = pd.merge(skater_game, goalie_game, how='outer')

  # filter df for only games in our time frame
  playsIn = playsIn.loc[playsIn["gameID"].isin(valid_game_ids)]

  # same - cuts data frame in half
  playsIn = playsIn.drop_duplicates()

  convert_column_int(playsIn, ["gameID", "playerID", "plusMinus"])
  return playsIn

def create_playsOn_df(games_df):
  skater_game = pd.read_csv(game_skater_stats)
  goalie_game = pd.read_csv(game_goalie_stats)

  skater_game = skater_game[["game_id", "player_id", "team_id"]]
  goalie_game = goalie_game[["game_id", "player_id", "team_id"]]

  players_game = pd.concat([skater_game, goalie_game], ignore_index=True)

  players_game.rename(columns={"game_id":"gameID", "player_id":"playerID", "team_id":"teamID"}, inplace=True)

  players_game = pd.merge(players_game, games_df, how="inner")
  
  playsOn = pd.DataFrame(columns=["teamID", "playerID", "startDate", "endDate"])

  players = players_game["playerID"].drop_duplicates()

  # print(len(players))

  for player_id in players:
    
    this_players_games = players_game.loc[players_game["playerID"] == player_id]
    this_players_games = this_players_games.sort_values(by="dateTime")

    curr_team = this_players_games.iloc[0]["teamID"]

    new_row = pd.DataFrame({"teamID":[curr_team], "playerID":[player_id], "startDate":[f"{FIRST_SEASON}-09-01"], "endDate":[pd.NA]})
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
  
  convert_column_int(playsOn, ["playerID", "teamID"])
      
  return playsOn

  
def create_officials_df():
  officials = pd.read_csv(game_officials)

  officials.rename(columns={"official_name":"officialName"}, inplace=True)
  officials = officials[["officialName"]].drop_duplicates()

  fix_apostrophes(officials)

  officials["officialID"] = range(1, len(officials) + 1)

  convert_column_int(officials, ["officialID"])

  return officials

# note: a couple games of 5 and 6 refs
def create_officiatedBy_df(officials_df, valid_game_ids):
  officials_games = pd.read_csv(game_officials)

  officials_games.rename(columns={"game_id": "gameID", "official_name":"officialName", "official_type": "officialType"}, inplace=True)
  officials_games = officials_games.drop_duplicates()

  fix_apostrophes(officials_games)

  # filter df for only games in our time frame
  officials_games = officials_games.loc[officials_games["gameID"].isin(valid_game_ids)]

  officials_games = pd.merge(officials_games, officials_df, on="officialName")
  officials_games = officials_games[["gameID", "officialID", "officialType"]].drop_duplicates()

  convert_column_int(officials_games, ["gameID", "officialID"])

  return officials_games


def create_shifts_df(valid_game_ids):
  shifts = pd.read_csv(game_shifts)

  # ------------remove------------ just for testing
  # shifts = shifts.iloc[:1000] 

  # Rename columns for consistency
  shifts = shifts.rename(columns={"game_id": "gameID", "player_id": "playerID", "shift_start": "shiftStart", "shift_end": "shiftEnd"})
  shifts = shifts[["gameID", "playerID", "shiftStart", "shiftEnd"]]

  shifts = shifts.loc[shifts["gameID"].isin(valid_game_ids)]

  # drop duplicates
  shifts = shifts.drop_duplicates()

  #630 shifts with missing endDates removed
  # print(len(shifts))
  shifts = shifts.dropna()
  # print(len(shifts))

  # Assign unique IDs for each shift
  shifts["shiftID"] = range(1, len(shifts) + 1)
  
  # Define the duration of each period in seconds
  PERIOD_DURATION = 1200

  # Calculate period number and period-relative shift start
  shifts["periodNumber"] = shifts["shiftStart"] // PERIOD_DURATION + 1
  shifts["shiftStart"] = shifts["shiftStart"] % PERIOD_DURATION
  shifts["shiftEnd"] = (shifts["shiftEnd"] % PERIOD_DURATION).astype(int)

  # ***Dont need dictionary anymore***
  # Create the dictionary mapper with (gameID, playerID, periodNumber, adjustedShiftStart) as the key
  # dict_shift_mapper = dict(zip(
  #     zip(shifts["gameID"], shifts["playerID"], shifts["periodNumber"], shifts["adjustedShiftStart"], shifts["adjustedShiftEnd"]),
  #     shifts["shiftID"]
  # ))

  convert_column_int(shifts, ["shiftID", "playerID", "gameID", "periodNumber", "shiftStart", "shiftEnd"])
  return shifts #, dict_shift_mapper

def create_plays_df(shifts_df, valid_game_ids):

  # filter games in timeframe
  plays_noPlayerID = pd.read_csv(game_plays)
  # rename the columns 
  plays_noPlayerID.rename(columns={"play_id": "playID", "game_id": "gameID", "period": "periodNumber", "event":"playType"}, inplace=True)
  valid_plays = ["Shot", "Goal", "Penalty"]
  plays_noPlayerID = plays_noPlayerID.loc[plays_noPlayerID["playType"].isin(valid_plays)]
  plays_noPlayerID = plays_noPlayerID.loc[plays_noPlayerID["gameID"].isin(valid_game_ids)]

  # filter 1) only important types of plays 2) games in timeframe,
  original_plays_playerID = pd.read_csv("../../data/game_plays_players.csv") 
  original_plays_playerID.rename(columns={"play_id": "playID", "player_id": "playerID"}, inplace=True)
  plays_playerID = original_plays_playerID.copy()
  values = ["PenaltyOn", "Scorer", "Shooter"]


  assists = plays_playerID.loc[plays_playerID["playerType"] == "Assist"]
  # print(assists)
  assists = assists[["playID", "playerID"]]
    
  plays_playerID = plays_playerID.loc[plays_playerID["playerType"].isin(values)]
  # plays_playerID = plays_playerID.loc[plays_playerID["gameID"].isin(valid_game_ids)] # not necessary because inner join
  plays_playerID = plays_playerID[["playID", "playerID"]] 

  # add playerID for goalie that saved the shot (and scored against)
  plays_goalie_save = original_plays_playerID.loc[original_plays_playerID["playerType"] == "Goalie"]
  plays_goalie_save.rename(columns={"playerID":"goalieID"}, inplace = True)
  plays_goalie_save = plays_goalie_save[["playID", "goalieID"]]
  plays_playerID = pd.merge(plays_playerID, plays_goalie_save, how="left", on="playID")
  

  # join the two csv files on the playID 
  plays = pd.merge(plays_noPlayerID, plays_playerID, how="inner", on="playID")

  # print(len(plays))
  # a lot of duplicates??? 352992 rows before 88248 after
  plays = plays.drop_duplicates()
  print(len(plays))

  # 4000 plays lost when merged?
  plays_shifts = pd.merge(plays, shifts_df, how="inner", on=["playerID", "gameID", "periodNumber"])

  plays = plays_shifts.loc[plays_shifts["periodTime"].between(plays_shifts["shiftStart"], plays_shifts["shiftEnd"])]
  # plays = plays_shifts.loc[plays_shifts[‘playTime’].between(plays_shifts[‘shiftStart’], plays_shifts[‘shiftEnd’])]

  small_shifts = plays[["gameID", "playerID", "shiftID", "periodNumber", "shiftStart", "shiftEnd"]]
  print(len(small_shifts))
  small_shifts = small_shifts.drop_duplicates()
  print(len(small_shifts))

  
  plays = plays[["playID", "playerID", "gameID", "shiftID", "periodNumber", 
                 "periodType", "periodTime", "playType", "secondaryType", "goalieID"]]
  
  plays["goalieID"] = plays["goalieID"].astype(pd.Int64Dtype())

  print(len(plays))
  plays = plays.drop_duplicates(subset='playID', keep='first')
  # This drops more - look into why??
  print(len(plays))

  assists = assists.loc[assists["playID"].isin(plays["playID"])]
  assists = assists.drop_duplicates()

  convert_column_int(assists, ["playerID"])

  convert_column_int(plays, ["shiftID", "playerID", "gameID", "periodNumber", "periodTime", "goalieID"])

  convert_column_int(small_shifts, ["shiftID", "playerID", "gameID", "periodNumber", "shiftStart", "shiftEnd"])

  return plays, small_shifts, assists


def create_inserts(df, table_name):

  col_names = ", ".join(df.columns)
  insert_string = f"INSERT INTO {table_name} ({col_names}) VALUES "

  individual_inserts = []

  for row in df.iterrows():
    values = []
    for val in row[1]:
      values.append(f"'{val}'" if isinstance(val, str) else str(val))

    values = ['NULL' if (x == 'nan' or x == '<NA>') else x for x in values] # replace missing values 'nan' with NULL

    values = ", ".join(values) # separate each value with comma and space
    individual_inserts.append(f"{insert_string}({values});\n")

  insert_string = "\n\n" + "".join(individual_inserts) #full insert statement for given table

  insert_string += f"\nPRINT('Table: {table_name} done inserting')\n"
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

def convert_column_int(df, columns):
  for col in columns:
    # print("casting: " + col)

    if(df[col].isnull().any()):
      df[col] = df[col].astype(pd.Int64Dtype())
    else:
      df[col] = df[col].astype(int)
      
      
# break string into chunks of 50,000 lines each
def split_chunks(sql_str, max_lines=50000):
    lines = sql_str.splitlines()
    chunks = []
    current_chunk = []

    for line in lines:
        current_chunk.append(line)
        # If the chunk reaches max_lines, finalize it
        if len(current_chunk) >= max_lines:
            chunks.append("\n".join(current_chunk))
            current_chunk = []

    # put any remaining lines as the last chunk
    if current_chunk:
        chunks.append("\n".join(current_chunk))

    return chunks
  
def create_meta_script(curr_directory, idx):
  
  BATCH_SIZE = 8 # number of SQL files to execute at once
  with open(f"sql_meta_insert.sql", "w") as file:
  
  # NOTE 
    for i in range(1, idx+1):
      if (i > 1 and i%(BATCH_SIZE) == 0):
        file.write("\n") # seperate batches 
      file.write("--:r "+curr_directory+f"\sql_chunk_{i}.sql\n")

def main():
  all_inserts = ""

  teams = create_teams_df()
  all_inserts += create_inserts(teams, "teams")
  print("done teams")

  venues, venueID_mapper = create_venues_df()
  all_inserts += create_inserts(venues, "venues")
  print("done venues")

  games = create_games_df(venueID_mapper)
  all_inserts += create_inserts(games, "games")
  print("done games")

  players = create_player_df()
  all_inserts += create_inserts(players, "players")
  print("done players")

  playsIn = create_playsIn_df(games["gameID"])
  all_inserts += create_inserts(playsIn, "playsIn")
  print("done playsIn")

  playsOn = create_playsOn_df(games)
  all_inserts += create_inserts(playsOn, "playsOn")
  print("done playsOn")

  officials = create_officials_df()
  all_inserts += create_inserts(officials, "officials")
  print("done officials")

  officiatedBy = create_officiatedBy_df(officials, games["gameID"])
  all_inserts += create_inserts(officiatedBy, "officiatedBy")
  print("done officiatedBy")

  shifts = create_shifts_df(games["gameID"])
  # all_inserts += create_inserts(shifts, "shifts")
  print("done shifts")

  plays, small_shifts, assists = create_plays_df(shifts, games["gameID"])
  all_inserts += create_inserts(small_shifts, "shifts")
  all_inserts += create_inserts(plays, "plays")
  all_inserts += create_inserts(assists, "assists")
  print("done plays")

  # with open('../inserts.sql', 'w') as file:
  #   file.write(SQL_CREATE_TABLES)
  #   file.write(all_inserts)
  # print("\nSQL file created successfully")
  
  # create chunks folder in workspace
  current_directory = os.getcwd()
  final_directory = os.path.join(current_directory, r'sql_chunks')
  if not os.path.exists(final_directory):
    os.makedirs(final_directory)
  
  MAX_LINES = 50000
  chunks = split_chunks(SQL_CREATE_TABLES + all_inserts, max_lines=MAX_LINES)
  for idx, chunk in enumerate(chunks, start=1):
      with open(f"sql_chunks\sql_chunk_{idx}.sql", "w") as file:
          file.write(chunk)
          print(f"chunk_{idx}.sql created")
  
  print("\nSQL chunks created successfully")
  
  
  create_meta_script(current_directory+"\sql_chunks", idx)
  print("\nSQL meta file created successfully")


main()


# ---- Says dont have permission to do this
# BULK INSERT MyTable
# FROM 'C:\Path\To\Your\File.csv'
# WITH (
#     FIELDTERMINATOR = ',', 
#     ROWTERMINATOR = '\n',
#     FIRSTROW = 2
# );