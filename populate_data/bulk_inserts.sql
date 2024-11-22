

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

BULK INSERT teams
FROM 'teams.csv'
WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2);