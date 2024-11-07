import pandas as pd

FIRST_SEASON = "2016"

SQL_CREATE_TABLES = """

USE cs3380;

DROP TABLE IF EXISTS playsIn;
DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS venues;
DROP TABLE IF EXISTS teams; 
DROP TABLE IF EXISTS skaters;
DROP TABLE IF EXISTS goalies;

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

CREATE TABLE skaters (
  playerID INT PRIMARY KEY,
  firstName varchar(30) NOT NULL,
  lastName varchar(30) NOT NULL,
  nationality varchar(30) NOT NULL,
  birthDate DATE NOT NULL,
  height varchar(30) NOT NULL,
  weight INT NOT NULL,
);

CREATE TABLE goalies (
  playerID INT PRIMARY KEY,
  firstName varchar(30) NOT NULL,
  lastName varchar(30) NOT NULL,
  nationality varchar(30) NOT NULL,
  birthDate DATE NOT NULL,
  height varchar(30) NOT NULL,
  weight INT NOT NULL,
);

CREATE TABLE playsIn (
  gameID INT,
  playerID INT,
  plusMinus INT NOT NULL,

  FOREIGN KEY (gameID) REFERENCES games (gameID)
    ON DELETE NO ACTION,
  FOREIGN KEY (playerID) REFERENCES skaters (playerID)
    ON DELETE NO ACTION,
  PRIMARY KEY (gameID, playerID)

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
  # Duplicate games whne inserting??
  games = pd.read_csv("../data/game.csv")

  games["venueID"] = games["venue"].map(venueID_mapper)
  games.rename(columns={"game_id":"gameID", "date_time_GMT":"dateTime", "home_team_id":"homeTeamID", "away_team_id":"awayTeamID"}, inplace=True)
  games = games[["gameID", "type", "dateTime", "outcome", "homeTeamID", "awayTeamID", "venueID"]]

  games["dateTime"] = games["dateTime"].str.replace("T", " ")
  games["dateTime"] = games["dateTime"].str.replace("Z", "")

  # filter games only keeping seasons after FIRST SEASON
  games = games.loc[(games["dateTime"].str[:4] >= FIRST_SEASON) & (games["dateTime"].str[5:7] >= "09")]

  # print(games.iloc[1])

  return games

def create_skaters_and_goalies_df():
  players = pd.read_csv("../data/player_info.csv")

  players.rename(columns={"player_id":"playerID"}, inplace=True)

  players = players[["playerID", "firstName", "lastName", "nationality", "birthDate", "height", "weight", "primaryPosition"]]
  # players["height"] = players["height"].str.replace("'", "''")
  fix_apostrophes(players)

  # removes 8 players with atleast one null values
  players = players[players.notnull().all(axis=1)]
  # print(players[players.isnull().any(axis=1)])

  skaters = players.loc[players["primaryPosition"] != "G"]
  skaters = skaters.drop(columns = ["primaryPosition"])

  goalies = players.loc[players["primaryPosition"] == "G"]
  goalies = goalies.drop(columns= ["primaryPosition"])

  return skaters, goalies

  
def create_playsIn_df(valid_game_ids):
  skater_game = pd.read_csv("../data/game_skater_stats.csv")

  print(skater_game.columns)


  skater_game.rename(columns={"game_id":"gameID", "player_id":"playerID"}, inplace=True)
  skater_game = skater_game[["gameID", "playerID", "plusMinus"]]

  # only have relationships for valid games

  skater_game = skater_game.loc[skater_game["gameID"].isin(valid_game_ids)]


  return skater_game




def create_inserts(df, table_name):

  col_names = ", ".join(df.columns)
  insert_string = f"INSERT INTO {table_name} ({col_names}) VALUES "

  individual_inserts = []

  for row in df.iterrows():
    values = []
    for val in row[1]:
      values.append(f"'{val}'" if isinstance(val, str) else str(val))

    values = ", ".join(values) # separate each value with comma and space
    individual_inserts.append(f"{insert_string}({values});\n")

  insert_string = "\n\n" + "".join(individual_inserts) #full insert statement for given table

  return insert_string


def fix_apostrophes(df):
  for i in range(df.shape[1]):
    if((df.iloc[:,i]).dtype == "object"):

      print((df.iloc[:,i]).dtype)
      df.iloc[:,i] = df.iloc[:,i].str.replace("'", "''")




def main():
  all_inserts = ""

  teams = create_teams_df()
  all_inserts += create_inserts(teams, "teams")

  venues, venueID_mapper = create_venues_df()
  all_inserts += create_inserts(venues, "venues")

  games = create_games_df(venueID_mapper)
  all_inserts += create_inserts(games, "games")

  skaters, goalies = create_skaters_and_goalies_df()
  all_inserts += create_inserts(skaters, "skaters")
  all_inserts += create_inserts(goalies, "goalies")

  playsIn = create_playsIn_df(games["gameID"])
  all_inserts += create_inserts(playsIn, "playsIn")

  with open('populate.sql', 'w') as file:
    file.write(SQL_CREATE_TABLES)
    file.write(all_inserts)

  print("\nSQL file created successfully")

main()


# ---- try
# BULK INSERT MyTable
# FROM 'C:\Path\To\Your\File.csv'
# WITH (
#     FIELDTERMINATOR = ',', 
#     ROWTERMINATOR = '\n',
#     FIRSTROW = 2
# );