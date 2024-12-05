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
game_plays_players = PATH + "game_plays_players.csv"

FIRST_SEASON = "2012"

# string to drop and create tables before inserting data
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
  venueName varchar(50) NOT NULL,
  teamID INT,

  FOREIGN KEY (teamID) REFERENCES teams (teamID)
    ON DELETE NO ACTION,
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
    CHECK (endDate IS NULL OR endDate >= startDate),

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
    playID varchar(30) PRIMARY KEY, 
    playerID INT, 
    gameID INT, 
    shiftID INT, 
    periodNumber INT,
    periodType varchar(15),
    periodTime INT, 
    playType varchar(15), 
         CHECK (playType IN ('Shot', 'Goal', 'Penalty')),
    secondaryType varchar(60), 
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

# create pandas dataframe for teams table
def create_teams_df():

  teams = pd.read_csv(team_info)
  teams.rename(columns={"shortName":"city", "team_id":"teamID"}, inplace=True)
  teams = teams[["teamID", "city", "teamName"]]

  # cleaning
  teams.loc[teams["city"].isin(["NY Rangers", "NY Islanders"]), "city"] = "New York"
  # remove old team
  teams = teams.loc[teams["teamName"] != "Thrashers"]
  convert_column_int(teams, ["teamID"])

  return teams


# create pandas dataframe for venues table
def create_venues_df():

  games = pd.read_csv(game)
  #only keep first occurence of each venue in games csv
  venues = games[["venue", "home_team_id"]].drop_duplicates(subset="venue", keep="first")
  venues.rename(columns={"venue":"venueName", "home_team_id":"teamID"}, inplace = True)
  # create venue ID
  venues["venueID"] = range(1, len(venues) + 1)

  # to fix apostrophes (two is one in a SQL string)
  fix_apostrophes(venues)
  # remove white space
  venues["venueName"] = venues["venueName"].str.replace("  ", "")

  # to use with games table function and have correct venueID
  dict_venue_mapper = dict(zip(venues["venueName"], venues["venueID"]))
  convert_column_int(venues, ["venueID", "teamID"])

  return venues, dict_venue_mapper

# create pandas dataframe for games table
def create_games_df(venueID_mapper):

  games = pd.read_csv(game)
  games["venueID"] = games["venue"].map(venueID_mapper)
  games.rename(columns={"game_id":"gameID", "date_time_GMT":"dateTime", "home_team_id":"homeTeamID", "away_team_id":"awayTeamID"}, inplace=True)
  games = games[["gameID", "type", "dateTime", "outcome", "season", "homeTeamID", "awayTeamID", "venueID"]]

  # format columns
  games["dateTime"] = games["dateTime"].str.replace("T", " ")
  games["dateTime"] = games["dateTime"].str.replace("Z", "")
  games["season"] = games["season"].astype(str)
  games["season"] = games["season"].str[:4] + "-" + games["season"].str[4:]

  # filter games only keeping seasons after FIRST SEASON
  games = games.loc[(games["dateTime"].str[:4] > FIRST_SEASON) | ((games["dateTime"].str[:4] == FIRST_SEASON) & (games["dateTime"].str[5:7] >= "09"))]

  games = games.drop_duplicates()
  # remove allstar games
  games = games.loc[games["type"] != "A"]
  convert_column_int(games, ["gameID", "homeTeamID", "awayTeamID", "venueID"])

  return games

# create pandas dataframe for players table
def create_player_df():

  players = pd.read_csv(player_info)
  players.rename(columns={"player_id":"playerID"}, inplace=True)
  players = players[["playerID", "firstName", "lastName", "nationality", "birthDate", "height", "weight", "primaryPosition"]]

  # clean - fix missing values
  fix_apostrophes(players)
  players.fillna({"weight":players["weight"].mean(), "height":"6'' 1\"", "nationality": "CAN"}, inplace=True)
  # 0 players removed now
  players = players[players.notnull().all(axis=1)]

  players["playerType"] = players["primaryPosition"].apply(lambda x: "Goalie" if x == "G" else "Skater")
  players = players.drop(columns = ["primaryPosition"])
  convert_column_int(players, ["playerID", "weight"])

  return players


# create pandas dataframe for playsIn table
def create_playsIn_df(valid_game_ids):

  skater_game = pd.read_csv(game_skater_stats)
  goalie_game = pd.read_csv(game_goalie_stats)
  skater_game.rename(columns={"game_id":"gameID", "player_id":"playerID"}, inplace=True)
  skater_game = skater_game[["gameID", "playerID", "plusMinus"]]
  goalie_game.rename(columns={"game_id":"gameID", "player_id":"playerID"}, inplace=True)
  goalie_game = goalie_game[["gameID", "playerID", "savePercentage"]]

  # join goalies and skaters into one df because inserting into one playsIn table
  playsIn = pd.merge(skater_game, goalie_game, how='outer')

  # filter df for only games in our time frame
  playsIn = playsIn.loc[playsIn["gameID"].isin(valid_game_ids)]
  playsIn = playsIn.drop_duplicates()
  convert_column_int(playsIn, ["gameID", "playerID", "plusMinus"])

  return playsIn

# create pandas dataframe for playsOn table
def create_playsOn_df(games_df):
  skater_game = pd.read_csv(game_skater_stats)
  goalie_game = pd.read_csv(game_goalie_stats)
  skater_game = skater_game[["game_id", "player_id", "team_id"]]
  goalie_game = goalie_game[["game_id", "player_id", "team_id"]]

  # one df with all both skaters and goalies
  players_game = pd.concat([skater_game, goalie_game], ignore_index=True)
  players_game.rename(columns={"game_id":"gameID", "player_id":"playerID", "team_id":"teamID"}, inplace=True)

  # join with games df to only consider relevant time period
  players_game = pd.merge(players_game, games_df, how="inner")

  # new df to fill
  playsOn = pd.DataFrame(columns=["teamID", "playerID", "startDate", "endDate"])

  # list of all players
  players = players_game["playerID"].drop_duplicates()

  # loop to create team relationships for each player one at atime
  for player_id in players:

    # all games this player played in
    this_players_games = players_game.loc[players_game["playerID"] == player_id]
    this_players_games = this_players_games.sort_values(by="dateTime")

    # current team is the team they played with during thier first game in the time frame we consider
    curr_team = this_players_games.iloc[0]["teamID"]
    new_row = pd.DataFrame({"teamID":[curr_team], "playerID":[player_id], "startDate":[f"{FIRST_SEASON}-09-01"], "endDate":[pd.NA]})
    playsOn = pd.concat([playsOn, new_row], ignore_index=True)

    # for all games - if the player is playing on a new team: end the current relationship with team and create a new one
    for row in this_players_games.iterrows():
      if(row[1]["teamID"] != curr_team):
        curr_team = row[1]["teamID"]
        new_team_date = row[1]["dateTime"][:10]
        playsOn.loc[len(playsOn) - 1, "endDate"] = new_team_date

        # **** endDate = NULL **** for the relationship indicates thta the player is still currently on that team
        new_row = pd.DataFrame({"teamID":[curr_team], "playerID":[player_id], "startDate":new_team_date, "endDate":[pd.NA]})
        playsOn = pd.concat([playsOn, new_row], ignore_index=True)

  convert_column_int(playsOn, ["playerID", "teamID"])
      
  return playsOn

  
# create pandas dataframe for officials table
def create_officials_df():

  officials = pd.read_csv(game_officials)
  officials.rename(columns={"official_name":"officialName"}, inplace=True)
  officials = officials[["officialName"]].drop_duplicates()
  fix_apostrophes(officials)

  # create officials id
  officials["officialID"] = range(1, len(officials) + 1)
  convert_column_int(officials, ["officialID"])

  return officials

# create pandas dataframe for officiatedBy table
def create_officiatedBy_df(officials_df, valid_game_ids):

  officials_games = pd.read_csv(game_officials)
  officials_games.rename(columns={"game_id": "gameID", "official_name":"officialName", "official_type": "officialType"}, inplace=True)
  officials_games = officials_games.drop_duplicates()
  fix_apostrophes(officials_games)

  # filter df for only games in our time frame
  officials_games = officials_games.loc[officials_games["gameID"].isin(valid_game_ids)]

  # join with officials df to get official id
  officials_games = pd.merge(officials_games, officials_df, on="officialName")

  officials_games = officials_games[["gameID", "officialID", "officialType"]].drop_duplicates()
  convert_column_int(officials_games, ["gameID", "officialID"])

  return officials_games

# create pandas dataframe for shifts table
def create_shifts_df(valid_game_ids):

  shifts = pd.read_csv(game_shifts)
  shifts = shifts.rename(columns={"game_id": "gameID", "player_id": "playerID", "shift_start": "shiftStart", "shift_end": "shiftEnd"})
  shifts = shifts[["gameID", "playerID", "shiftStart", "shiftEnd"]]

  # only consider shifts in our timeframe of games
  shifts = shifts.loc[shifts["gameID"].isin(valid_game_ids)]

  # clean
  shifts = shifts.drop_duplicates()
  shifts = shifts.dropna()

  # Assign unique IDs for each shift
  shifts["shiftID"] = range(1, len(shifts) + 1)
  
  # Define the duration of each period in seconds
  PERIOD_DURATION = 1200

  # Calculate period number and period-relative shift start in seconds during that period not total seconds in game
  shifts["periodNumber"] = shifts["shiftStart"] // PERIOD_DURATION + 1
  shifts["shiftStart"] = shifts["shiftStart"] % PERIOD_DURATION
  shifts["shiftEnd"] = (shifts["shiftEnd"] % PERIOD_DURATION).astype(int)

  convert_column_int(shifts, ["shiftID", "playerID", "gameID", "periodNumber", "shiftStart", "shiftEnd"])

  return shifts

# create pandas dataframe for plays, shifts, and assists table
def create_plays_df(shifts_df, valid_game_ids):

  # get df for play descriptions
  plays_noPlayerID = pd.read_csv(game_plays)
  plays_noPlayerID.rename(columns={"play_id": "playID", "game_id": "gameID", "period": "periodNumber", "event":"playType"}, inplace=True)

  # filter to only consider valid play types in games within out timeframe
  valid_plays = ["Shot", "Goal", "Penalty"]
  plays_noPlayerID = plays_noPlayerID.loc[plays_noPlayerID["playType"].isin(valid_plays)]
  plays_noPlayerID = plays_noPlayerID.loc[plays_noPlayerID["gameID"].isin(valid_game_ids)]

  # get df to link players to a play
  original_plays_playerID = pd.read_csv(game_plays_players) 
  original_plays_playerID.rename(columns={"play_id": "playID", "player_id": "playerID"}, inplace=True)
  plays_playerID = original_plays_playerID.copy()
  
  # get assists for all goals
  assists = plays_playerID.loc[plays_playerID["playerType"] == "Assist"]
  assists = assists[["playID", "playerID"]]

  # only consider the player that made the play
  values = ["PenaltyOn", "Scorer", "Shooter"]
  plays_playerID = plays_playerID.loc[plays_playerID["playerType"].isin(values)]
  plays_playerID = plays_playerID[["playID", "playerID"]] 

  # add playerID for goalie that saved the shot (and scored against)
  plays_goalie_save = original_plays_playerID.loc[original_plays_playerID["playerType"] == "Goalie"].copy()
  plays_goalie_save.rename(columns={"playerID":"goalieID"}, inplace = True)
  plays_goalie_save = plays_goalie_save[["playID", "goalieID"]]
  plays_playerID = pd.merge(plays_playerID, plays_goalie_save, how="left", on="playID")
  
  # join the two dfs on the playID to get the play and player that made the play
  plays = pd.merge(plays_noPlayerID, plays_playerID, how="inner", on="playID")
  plays = plays.drop_duplicates()

  # only consider plays that have a valid shift from that player (about 4000 lost)
  plays_shifts = pd.merge(plays, shifts_df, how="inner", on=["playerID", "gameID", "periodNumber"])
  # keep the shiftID for which that play occured
  plays = plays_shifts.loc[plays_shifts["periodTime"].between(plays_shifts["shiftStart"], plays_shifts["shiftEnd"])]
 
  # dont use shifts without plays so only keeping shifts that have atleast one play during it so can have more season's data
  shifts_with_plays = plays[["gameID", "playerID", "shiftID", "periodNumber", "shiftStart", "shiftEnd"]]
  shifts_with_plays = shifts_with_plays.drop_duplicates()

  plays = plays[["playID", "playerID", "gameID", "shiftID", "periodNumber", 
                 "periodType", "periodTime", "playType", "secondaryType", "goalieID"]]
  plays = plays.drop_duplicates(subset='playID', keep='first')

  assists = assists.loc[assists["playID"].isin(plays["playID"])]
  assists = assists.drop_duplicates()

  convert_column_int(assists, ["playerID"])
  convert_column_int(plays, ["shiftID", "playerID", "gameID", "periodNumber", "periodTime", "goalieID"])
  convert_column_int(shifts_with_plays, ["shiftID", "playerID", "gameID", "periodNumber", "shiftStart", "shiftEnd"])

  return plays, shifts_with_plays, assists

# returns a string with insert statements for each line of the pandas df 
# inserting into table with name table_name
def create_inserts(df, table_name):

  col_names = ", ".join(df.columns)
  insert_string = f"INSERT INTO {table_name} ({col_names}) VALUES "

  individual_inserts = []

  # create insert statement one row at a time
  for row in df.iterrows():
    values = []
    for val in row[1]:
      values.append(f"'{val}'" if isinstance(val, str) else str(val))

    values = ['NULL' if (x == 'nan' or x == '<NA>') else x for x in values] # replace missing values 'nan' with NULL

    values = ", ".join(values) # separate each attribute value with comma and space
    individual_inserts.append(f"{insert_string}({values});\n")

  insert_string = "\n\n" + "".join(individual_inserts) # all insert statements for given table separated by newline characters
  insert_string += f"\nPRINT('Table: {table_name} done inserting')\n"
  
  return insert_string

# to turn an apostrophe in text to two apostrophes for SQL string
def fix_apostrophes(df):
  for i in range(df.shape[1]):
    if((df.iloc[:,i]).dtype == "object"):
      df.iloc[:,i] = df.iloc[:,i].str.replace("'", "''")

# convert all interger columns to ints to prevent decimal places
def convert_column_int(df, columns):
  for col in columns:
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

# create all pandas dataframes and create corresponding insert statements 
# all_inserts is a string with all inserts for all tables
def main():
  all_inserts = ""

  teams = create_teams_df()
  all_inserts += create_inserts(teams, "teams")
  print("teams inserts created successfully")

  venues, venueID_mapper = create_venues_df()
  # don't include Thrashers arenas
  venues = venues.loc[venues["teamID"].isin(teams["teamID"])]
  all_inserts += create_inserts(venues, "venues")
  print("venues inserts created successfully")

  games = create_games_df(venueID_mapper)
  all_inserts += create_inserts(games, "games")
  print("games inserts created successfully")

  players = create_player_df()
  playsOn = create_playsOn_df(games)
  # filter players to only keep players who are on a team
  players = players.loc[players["playerID"].isin(playsOn["playerID"])]

  all_inserts += create_inserts(players, "players")
  print("players inserts created successfully")

  playsIn = create_playsIn_df(games["gameID"])
  all_inserts += create_inserts(playsIn, "playsIn")
  print("playsIn inserts created successfully")
  
  all_inserts += create_inserts(playsOn, "playsOn")
  print("playsOn inserts created successfully")

  officials = create_officials_df()
  all_inserts += create_inserts(officials, "officials")
  print("officials inserts created successfully")

  officiatedBy = create_officiatedBy_df(officials, games["gameID"])
  all_inserts += create_inserts(officiatedBy, "officiatedBy")
  print("officiatedBy inserts created successfully")

  shifts = create_shifts_df(games["gameID"])
  # all_inserts += create_inserts(shifts, "shifts")
  print("shifts df created successfully")

  plays, small_shifts, assists = create_plays_df(shifts, games["gameID"])
  all_inserts += create_inserts(small_shifts, "shifts")
  all_inserts += create_inserts(plays, "plays")
  all_inserts += create_inserts(assists, "assists")
  print("shifts, plays, and assists inserts created successfully")
  
  # create chunks folder in workspace
  current_directory = os.getcwd()
  final_directory = os.path.join(current_directory, r'sql_chunks')
  if not os.path.exists(final_directory):
    os.makedirs(final_directory)
  
  # write chunks (i.e., sql files with 50,000 lines each) to chunks folder
  MAX_LINES = 50000
  chunks = split_chunks(SQL_CREATE_TABLES + all_inserts, max_lines=MAX_LINES)
  for idx, chunk in enumerate(chunks, start=1):
      with open(f"sql_chunks/sql_chunk_{idx}.sql", "w") as file:
          file.write("SET NOCOUNT ON;\n")
          file.write(chunk)
          print(f"chunk_{idx}.sql created")
  
  print("\nSQL chunks created successfully")
  
main()