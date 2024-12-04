import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/*
 * Queries Implemented:
 *  
 * (1) total goals against each team for a player
 * (2) total goals, assists, points for player
 * (3) avg shift length before the player scores, gets shot, or penalty
 * (4) total goals score at all venues
 * (5) top N officials calling most penalties against away teams
 * (6) Top N players having played on the most teams 
 * (7) top N players who have taken the most penalities
 * (8) average shift length per period
 * (9) total play off wins for a team X in season Y
 * (10) players who have scored against all teams except their current team
 * (11) top25 by goals/assists/points/plusMinus
 * (12) goals per shot for all players, descending order 
 * (13) all Teams
 * (14) search for a player by name
 * (15) a team's game schedule
 * (16) players with the most gordie howe hat tricks
 */

public class HockeyDB {
    private Connection connection;

    public HockeyDB() {
        Properties prop = new Properties();
        String fileName = "../../data/auth.cfg";
        try {
            FileInputStream configFile = new FileInputStream(fileName);
            prop.load(configFile);
            configFile.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Could not find config file.");
            System.exit(1);
        } catch (IOException ex) {
            System.out.println("Error reading config file.");
            System.exit(1);
        }
        String username = (prop.getProperty("username"));
        String password = (prop.getProperty("password"));

        if (username == null || password == null) {
            System.out.println("Username or password not provided.");
            System.exit(1);
        }

        String connectionUrl = "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                + "database=cs3380;"
                + "user=" + username + ";"
                + "password=" + password + ";"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        try {
            // create a connection to the database
            connection = DriverManager.getConnection(connectionUrl);

        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }

    }

    public void example() {
        String steps = """
                    Try the following examples set of commands to get started!
                    
                    1) Enter 'sp'
                    2) Enter 'claude' when prompted for a name
                    3) See 'Claude Giroux' in the resulting list
                    4) Enter 'tgap'
                    5) Enter 'Claude' when promoted for a first name
                    6) Enter 'Giroux' when prompted for a last name
                    7) See Claude Giroux's player stats in the resulting table

                    Congratulations! You have succesfully completed your first set of commands in the NHL database.
                 """;

        System.out.println(steps);
    }
    

    // (1)
    public void totalGoalsByTeam(String first, String last) {
        if (!playerExists(first, last)) {
            return;
        }

        try {
                    String sql = """
	                -- Goals scored on teams that were home
						with homeGoals as (
                            SELECT t.teamName,
                                COUNT(CASE 
                                    WHEN p.playType = 'Goal' 
                                        AND (
                                            po.teamID = g.awayTeamID
                                            AND g.dateTime BETWEEN po.startDate AND ISNULL(po.endDate, GETDATE())
                                        )
                                    THEN 1 
                                    ELSE NULL 
                                END) AS numGoals
                            FROM teams t
                            LEFT JOIN games g ON t.teamID = g.homeTeamID
                            LEFT JOIN plays p ON g.gameID = p.gameID
                            LEFT JOIN players scorer ON p.playerID = scorer.playerID
                            LEFT JOIN playsOn po ON scorer.playerID = po.playerID
                            WHERE scorer.firstName = ? AND scorer.lastName = ?
                            GROUP BY t.teamName
						),

						-- Goals scored on teams that were away
						awayGoals as (
                            SELECT t.teamName,
                                COUNT(CASE 
                                    WHEN p.playType = 'Goal' 
                                        AND (
                                            po.teamID = g.homeTeamID
                                            AND g.dateTime BETWEEN po.startDate AND ISNULL(po.endDate, GETDATE())
                                        )
                                    THEN 1 
                                    ELSE NULL 
                                END) AS numGoals
                            FROM teams t
                            LEFT JOIN games g ON t.teamID = g.awayTeamID
                            LEFT JOIN plays p ON g.gameID = p.gameID
                            LEFT JOIN players scorer ON p.playerID = scorer.playerID
                            LEFT JOIN playsOn po ON scorer.playerID = po.playerID
                            WHERE scorer.firstName = ? AND scorer.lastName = ?
                            GROUP BY t.teamName
						)

						SELECT teamName, sum(numGoals) goalTotal FROM 
						(SELECT teamName, numGoals FROM homeGoals UNION ALL SELECT teamName, numGoals FROM awayGoals) x 
						GROUP BY teamName ORDER BY goalTotal DESC;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, first);
            pstmt.setString(2, last);
            pstmt.setString(3, first);
            pstmt.setString(4, last);

            ResultSet rs = pstmt.executeQuery();

            
            printBoxedText(String.format("Goals against each team for %s %s", first, last));
            String[] titles = { "Team Name", "Goals Scored" };
            TablePrinter.printResultSet(rs, titles);

            // final int[] SPACINGS = { 16 }; // SPACINGS[[i] is width of i'th column
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // int totalGoals = 0;
            // while (rs.next()) {
            //     String[] columns = { rs.getString(1), rs.getString(2) };
            //     printTitles(columns, SPACINGS);
            //     totalGoals += rs.getInt(2);
            // }

            // // display total goals after printing
            // printDashes(titles, SPACINGS);
            // String[] totals = { "Total:", "" + totalGoals };
            // printTitles(totals, SPACINGS);
            // System.out.println();

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (2)
    public void totalGAP(String first, String last) {

        if (!playerExists(first, last)) {
            return;
        }

        try {

            String sql = """
                        with allGoals AS (
                            SELECT players.playerID, players.firstName, players.lastName, season,
                            count(plays.playID) as totalGoals
                            FROM plays
                            JOIN games ON games.gameID = plays.gameID
                            JOIN players ON plays.playerID = players.playerID
                            WHERE playType = 'Goal'
                            Group by players.playerID, players.firstName, players.lastName, season
                        ),
                        allAssists AS (
                            select assists.playerID, season, count(assists.playerID) as totalAssists
                            FROM assists
                            join plays on plays.playid = assists.playID
                            JOIN games on games.gameID = plays.gameID
                            where playType = 'Goal'
                            Group by assists.playerID, season
                        )
                        SELECT allGoals.playerID, allGoals.season, totalGoals, totalAssists,
                        sum(totalGoals + totalAssists) as totalPoints
                        from allAssists join allGoals on allAssists.playerID = allgoals.playerID
                        AND allAssists.season = allGoals.season
                        WHERE firstName = ? And lastName = ?
                        group by allGoals.playerID, firstName, lastName, totalGoals, totalAssists, allGoals.season
                        order by allGoals.playerID, allGoals.season desc;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, first);
            pstmt.setString(2, last);

            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Total Goals, Assists, and Points for %s %s", first, last));

            String[] titles = { "Player ID", "Season", "Goals", "Assists", "Points" };
            TablePrinter.printResultSet(rs, titles);


            // final int[] SPACINGS = { 11, 12, 8, 9 }; // SPACINGS[[i] is width of i'th column
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];

            // while (rs.next()) {
            //     for (int i = 1; i <= titles.length; i++) {
            //         columns[i - 1] = rs.getString(i);
            //     }
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }

    }

    // (3)
    public void avgShiftByPlay() {
        try {

            String sql = """
                        SELECT plays.playType, AVG(shiftEnd - shiftStart) avgShiftLength
                        FROM plays
                        JOIN shifts ON shifts.shiftID = plays.shiftID
                        GROUP BY plays.playType;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            printBoxedText("Avg. shift length for each play type");
            String[] titles = { "Play Type", "Shift Length" };
            TablePrinter.printResultSet(rs, titles);

            // final int[] SPACINGS = { 12 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // while (rs.next()) {
            //     columns[0] = rs.getString("playType");
            //     columns[1] = getMins(rs.getInt("avgShiftLength"));
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }

    }

    // (4)
    public void goalsByVenue(String season) {
        try {

            String sql = """
                        SELECT venues.venueName, COUNT(*) as numGoals
                        FROM plays
                        JOIN games ON plays.gameID = games.gameID
                        JOIN venues ON games.venueID = venues.venueID
                        WHERE games.season = ? AND plays.playType = 'Goal'
                        GROUP BY venues.venueID, venues.venueName
                        ORDER BY numGoals DESC;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, season);
            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Total goals scored at each venue for the year %s", season));
            String[] titles = { "Venue Name", "Total Goals" };
            TablePrinter.printResultSet(rs, titles);

            // final int[] SPACINGS = { 28 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // while (rs.next()) {
            //     columns[0] = rs.getString("venueName");
            //     columns[1] = rs.getString("numGoals");
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }

    }

    // (5)
    public void topNOfficialPenalties(int numRows) {
        try {

            String sql = """
                        WITH allAwayTeamPenalties AS (
                            SELECT plays.gameID
                            FROM plays
                            JOIN games ON plays.gameID = games.gameID
                            JOIN playsOn ON plays.playerID = playson.playerID
                            WHERE plays.playType = 'Penalty'
                            AND playsOn.teamID = games.awayteamID
                        )

                        SELECT officials.officialName, COUNT(*) AS numPenalties
                        FROM officiatedby
                        JOIN officials ON officiatedBy.officialID = officials.officialID
                        JOIN allAwayTeamPenalties ON allAwayTeamPenalties.gameID = officiatedBy.gameID
                        WHERE officiatedBy.officialType = 'Referee'
                        GROUP BY officials.officialID, officials.officialName
                        ORDER BY numPenalties DESC;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            // pstmt.setInt(1, numRows); // Doesn't allow a placeholder parameter next to
            // top see FIX below

            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Top %d officials who call the most penalites against away teams", numRows));
            String[] titles = {"Rank", "Name", "Penalties Called" };
            TablePrinter.printResultSetWithRank(rs, titles, numRows);
            
            
            // final int[] SPACINGS = { 6, 22, 16 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // int rank = 1;
            // while (rs.next() && rank <= numRows) { // FIX: print until rank equals desired numRows
            //     columns[0] = "" + rank;
            //     // populate each row before printing it
            //     for (int i = 1; i < titles.length; i++) {
            //         columns[i] = rs.getString(i);
            //     }
            //     printTitles(columns, SPACINGS);
            //     rank++;
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (6)
    public void topTeamsPlayedFor(int numRows) {
        try {

            String sql = """
                        SELECT firstName, lastName, COUNT(teamID) as numTeams 
                        FROM playsOn  
                        JOIN players ON playsOn.playerID = players.playerID 
                        GROUP BY players.playerID, firstName, lastName 
                        ORDER BY numTeams DESC; 
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            // pstmt.setInt(1, numRows); // Doesn't allow a placeholder parameter next to
            // top see FIX below

            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Top %d players who have played for the most teams", numRows));
            String[] titles = { "Rank", "First", "Last", "No. Teams" };
            TablePrinter.printResultSetWithRank(rs, titles, numRows);
            
            
            // final int[] SPACINGS = { 6, 16, 15 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // int rank = 1;
            // while (rs.next() && rank <= numRows) { // FIX: print until rank equals desired numRows
            //     columns[0] = "" + rank;
            //     // populate each row before printing it
            //     for (int i = 1; i < titles.length; i++) {
            //         columns[i] = rs.getString(i);
            //     }
            //     printTitles(columns, SPACINGS);
            //     rank++;
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (7)
    public void topPlayersPenalties(int numRows) {
        try {

            String sql = """
                        SELECT players.firstname, players.lastname, players.height, players.weight, COUNT(plays.playID) as numberOfPenalties 
                        FROM players  
                        JOIN plays ON players.playerID = plays.playerID 
                        WHERE plays.playType = 'Penalty' 
                        GROUP BY players.firstname, players.lastname, players.weight, players.height 
                        ORDER BY numberOfPenalties DESC; 
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            // pstmt.setInt(1, numRows); // Doesn't allow a placeholder parameter next to
            // top see FIX below

            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Top %d players who have taken the most penalites", numRows));
            String[] titles = { "Rank", "First", "Last" , "Height", "Weight", "No. Penalties"};
            TablePrinter.printResultSetWithRank(rs, titles, numRows);

            // final int[] SPACINGS = { 6, 16, 15 , 10, 8};
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // int rank = 1;
            // while (rs.next() && rank <= numRows) { // FIX: print until rank equals desired numRows
            //     columns[0] = "" + rank;
            //     // populate each row before printing it
            //     for (int i = 1; i < titles.length; i++) {
            //         columns[i] = rs.getString(i);
            //     }
            //     printTitles(columns, SPACINGS);
            //     rank++;
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (8)
    public void avgShiftLengthByPeriod() {
        try {

            String sql = """
                        SELECT periodNumber, AVG(shiftEnd - shiftStart) as shiftLength
                        FROM shifts
                        GROUP BY periodNumber
                        ORDER BY periodNumber ASC
                    """;


            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Average shift length by period"));
            String[] titles = { "Period", "Shift Length" };
            TablePrinter.printResultSet(rs, titles);

            // final int[] SPACINGS = { 8, 12 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // while (rs.next()) {
            //     columns[0] = rs.getString(1);
            //     columns[1] = "" + getMins(rs.getInt(2));
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (9)
    public void totalPlayoffWins(String teamName, String season) {

        if (!teamExists(teamName)) {
            return;
        }

        try {

            String sql = """
                    WITH homePloffWins AS (
                    SELECT gameID, teamID, teamName
                    FROM games
                    JOIN teams ON games.homeTeamID = teams.teamID
                    WHERE
                    teams.teamName = ?
                    AND games.season = ?
                    AND games.type = 'P'
                    AND (games.outcome = 'home win reg' OR games.outcome = 'home win ot')
                    ),

                    awayPloffWins AS (
                    SELECT gameID, teamID, teamName
                    FROM games
                    JOIN teams ON games.awayTeamID = teams.teamID
                    WHERE
                    teams.teamName = ?
                    AND games.season = ?
                    AND games.type = 'P'
                    AND (games.outcome = 'away win reg' OR games.outcome = 'away win ot')
                    ),

                    HomeAwayPloffWins AS (
                    SELECT *
                    FROM homePloffWins
                    UNION
                    SELECT *
                    FROM awayPloffWIns
                    )

                    SELECT COUNT(*) as totalPlayoffWins, 16 as max_possible FROM HomeAwayPloffWins;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, teamName);
            pstmt.setString(2, season);
            pstmt.setString(3, teamName);
            pstmt.setString(4, season);

            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Total playoff wins for the %s in the %s season", teamName, season));

            String[] titles = { "Wins", "Maximum" };
            TablePrinter.printResultSet(rs, titles);
            // final int[] SPACINGS = { 10 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // while (rs.next()){
            //     String[] columns = { rs.getString(1), rs.getString(2) };
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();

        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (10)
    public void playersScoredAgainstAllTeams() {
        try {

            String sql = """
                        SELECT firstName, lastName  
                        FROM players 
                        WHERE NOT EXISTS (  
                        
                        SELECT teamID FROM teams 
                        EXCEPT   
                        ( 
                        -- player's current team 
                        SELECT teamID 
                        FROM playsOn 
                        WHERE playsOn.playerID = players.playerID 
                        AND endDate IS NULL 

                        UNION  
                        -- all teams this player has scored against 
                        SELECT DISTINCT IIF(games.homeTeamID = playsOn.teamID, games.awayTeamID, games.homeTeamID) AS teamID -- teams that player has scored against  
                        FROM plays  
                        JOIN games ON plays.gameID = games.gameID  
                        JOIN playsOn ON plays.playerID = playsOn.playerID 
                        WHERE plays.playType = 'goal' AND (games.dateTime >= playsOn.startdate AND (playsOn.endDate IS NULL OR games.dateTime <= playsOn.endDate)) AND plays.playerID = players.playerID)); 
                    """;


            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Players who have scored against all teams"));
            String[] titles = { "First", "Last" };
            TablePrinter.printResultSet(rs, titles);
            
            
            // final int[] SPACINGS = { 16, 15 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // while (rs.next()) {
            //     columns[0] = rs.getString(1);
            //     columns[1] = rs.getString(2);
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (11)
    public void top25byStat(String statType, String season) {
        try {

            String sql = """
                    		WITH allGoals AS (
                    			SELECT playerID, COUNT(*) as numGoals
                    			FROM plays
                    			JOIN games ON plays.gameID = games.gameID
                    			WHERE plays.playType = 'goal'
                    			AND games.season = ?
                    			GROUP BY playerID
                    		),
                    		allAssists AS (
                    			SELECT assists.playerID, COUNT(*) as numAssists
                    			FROM assists
                    			JOIN plays ON assists.playID = plays.playID
                    			JOIN games ON games.gameID = plays.gameID
                    			AND games.season = ?
                    			GROUP BY assists.playerID

                    		),
                    		totalPlusMinus AS (
                    			SELECT playsIn.playerID, SUM(playsIn.plusMinus) as plusMinus
                    			FROM playsIn
                    			JOIN games ON games.gameID = playsIn.gameID
                    			WHERE games.season = ?
                    			GROUP BY playsIn.playerID
                    		),
                    		totals AS (
                    			SELECT allGoals.playerID, numGoals, numAssists, (numGoals + numAssists) AS numPoints, plusMinus
                    			FROM allGoals
                    			JOIN allAssists ON allGoals.playerID = allAssists.playerID
                    			JOIN totalPlusMinus ON allGoals.playerID = totalPlusMinus.playerID
                    		)
                    		SELECT TOP 25 players.firstName, players.lastName, numGoals, numAssists, numPoints, plusMinus
                    		FROM totals
                    		JOIN players ON totals.playerID = players.playerID
                    """;
            sql += "ORDER BY " + getStatSQL(statType) + " DESC;";

            PreparedStatement pstmt = connection.prepareStatement(sql);

            for (int i = 1; i <= 3; i++)
                pstmt.setString(i, season);

            ResultSet rs = pstmt.executeQuery();

            final int NUM_ROWS = 25;
            printBoxedText(String.format("Top 25 Players ordered by %s", getStat(statType)));
            String[] titles = { "Rank", "First", "Last", "Goals", "Assists", "Points", "Plus Minus" };
            TablePrinter.printResultSetWithRank(rs, titles, NUM_ROWS);
            
            // final int[] SPACINGS = { 6, 14, 15, 8, 9, 8 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // int rank = 1;
            // while (rs.next()) {
            //     columns[0] = "" + rank;
            //     // populate each row before printing it
            //     for (int i = 1; i < titles.length; i++) {
            //         columns[i] = rs.getString(i);
            //     }
            //     printTitles(columns, SPACINGS);
            //     rank++;
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (12)
    public void goalsPerShotAllPlayers() {
        try {

            String sql = """
                        WITH playerGoals AS (  
                            SELECT players.playerID, firstname, lastname, COUNT(plays.playID) AS goals  
                            FROM players  
                            LEFT JOIN plays ON players.playerID = plays.playerID   
                            WHERE playType = 'Goal' 
                            GROUP BY players.playerID, players.firstname, players.lastname 
                        ),  
                        
                        playerShots AS (  
                            SELECT players.playerID, firstname as fName, lastname as lName, COUNT(plays.playID) AS shots  
                            FROM players  
                            LEFT JOIN plays ON players.playerID = plays.playerID  
                            WHERE playType = 'Shot' 
                            GROUP BY players.playerID, firstname, lastname 
                        )  
                            
                        SELECT firstName, lastName, (CAST(goals AS REAL) / shots) AS goals_per_shot_average  
                        FROM playerGoals  
                        JOIN playerShots ON playerGoals.playerID = playerShots.playerID  
                        ORDER BY goals_per_shot_average DESC;
                    """;


            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Players goals per shot average"));
            String[] titles = { "First", "Last", "Goals Per Shot" };
            TablePrinter.printResultSet(rs, titles);
            
            // final int[] SPACINGS = { 16, 15 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // while (rs.next()) {
            //     columns[0] = rs.getString(1);
            //     columns[1] = rs.getString(2);
            //     columns[2] = rs.getString(3);
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (13)
    public void allTeams() {
        try {

            String sql = """
                    SELECT teamID, city, teamName
                    FROM teams;
                    """;

            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();

            printBoxedText("All NHL Teams");

            String[] titles = { "ID", "City", "Team Name" };
            TablePrinter.printResultSet(resultSet, titles);
           
           
            // final int[] SPACINGS = { 6, 15 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // while (resultSet.next()) {
            //     for (int i = 1; i <= columns.length; i++) {
            //         columns[i - 1] = resultSet.getString(i);
            //     }
            //     printTitles(columns, SPACINGS);
            // }

            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

     // (14)
    public void searchPlayer(String name) {

        try {

            String sql = """
                        SELECT firstName, lastName, playerType, nationality, birthDate, height, weight 
                        FROM players 
                        WHERE firstname LIKE ?  
                        OR lastname LIKE ?  
                        OR CONCAT(firstname, ' ', lastName) LIKE ? 
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, "%" + name + "%");
            pstmt.setString(2, "%" + name + "%");
            pstmt.setString(3, "%" + name + "%");

            ResultSet rs = pstmt.executeQuery();

            // added to print out alternative message if no matches are found
            if (!rs.isBeforeFirst()) {
                printBoxedText(String.format("Sorry there are no players matching the name '%s'", name));
            } else {
        
                printBoxedText(String.format("Players with a name matching '%s'", name));

                String[] titles = { "First", "Last", "Player Type", "Nationality", "Date of Birth", "Height", "Weight" };
                TablePrinter.printResultSet(rs, titles);
                // final int[] SPACINGS = { 16, 15, 14, 14, 16, 10 }; // SPACINGS[[i] is width of i'th column
                // printTitles(titles, SPACINGS);
                // printDashes(titles, SPACINGS);

                // String[] columns = new String[titles.length];

                // while (rs.next()) {
                //     for (int i = 1; i <= titles.length; i++) {
                //         columns[i - 1] = rs.getString(i);
                //     }
                //     printTitles(columns, SPACINGS);
                // }
            }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }

    }

    // (15)
    public void schedule(String teamName, String season) {
        if (!teamExists(teamName)) {
            return;
        }

        try {
            String[] years = season.split("-");
            // Typical reg season spans from early october to mid april, 
            // used september and june as wide parameters to ensure all games are acounted for
            String firstHalfSeasonStart = years[0]+"-09-01"; //Early september
            String lastHalfSeasonEnd =  years[1]+"-07-01"; //Early Juner

            String sql = """
                    WITH homeTeamGames AS (
                        SELECT * FROM teams JOIN games ON teams.teamID = games.homeTeamID 
                        WHERE teams.teamName = ? AND games.dateTime between ? AND ?  AND games.type = 'R'
                    ),

                    awayTeamGames AS (
                        SELECT * FROM teams JOIN games on teams.teamID = games.awayTeamID
                        WHERE teams.teamName = ? AND games.dateTime between ? AND ? AND games.type = 'R'
                    ),

                    seasonGames AS (
                        SELECT homeTeamGames.teamName homeTeam, teams.teamName awayTeam, homeTeamGames.dateTime
                        FROM homeTeamGames JOIN teams ON homeTeamGames.awayTeamID = teams.teamID
                        UNION
                        SELECT teams.teamName homeTeam, awayTeamGames.teamName awayTeam, awayTeamGames.dateTime
                        FROM awayTeamGames JOIN teams ON awayTeamGames.homeTeamID = teams.teamID
                    )

                    SELECT homeTeam, awayTeam, CAST(DATEADD(HOUR, -6, [dateTime]) AS DATE) AS date, CONVERT(TIME(0), DATEADD(HOUR, -6, [dateTime])) AS time FROM seasonGames 
                    ORDER BY dateTime;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, teamName);
            pstmt.setString(2, firstHalfSeasonStart);
            pstmt.setString(3, lastHalfSeasonEnd);
            pstmt.setString(4, teamName);
            pstmt.setString(5, firstHalfSeasonStart);
            pstmt.setString(6, lastHalfSeasonEnd);
  

            ResultSet rs = pstmt.executeQuery();


            printBoxedText(String.format("%s schedule for the %s season", teamName, season));

            String[] titles = { "Home Team", "Away Team", "Date", "Time (CST)" };
            TablePrinter.printResultSet(rs, titles);
            // final int[] SPACINGS = { 16, 16, 14 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // while (rs.next()){
            //     String[] columns = { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4) };
            //         printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();

        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (16)
    public void gordieHoweHatTrick() {
        try {

            String sql = """
                   WITH gordHats AS (
                    SELECT players.playerID, players.firstname, players.lastname, outerPlays.gameID
                    FROM players 
                    JOIN plays outerPlays ON players.playerID = outerPlays.playerID
                    WHERE playType = 'Goal'
                    AND players.playerID IN (
                        SELECT innerPlays.playerID
                        FROM plays innerPlays
                        WHERE innerPlays.gameID = outerPlays.gameID
                        AND innerPlays.playType = 'Penalty'
                    )
                    AND players.playerID IN (
                        SELECT assists.playerID
                        FROM assists
                        JOIN plays innerPlays ON assists.playID = innerPlays.playID
                        WHERE innerPlays.gameID = outerPlays.gameID
                    )
                )

                SELECT firstname, lastname, COUNT(*) as numGordHats
                FROM gordHats
                GROUP BY playerID, firstname, lastname
                ORDER BY numGordHats DESC;
            """;


            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Players with the most Gordie Howe Hat Tricks"));
            String[] titles = { "First", "Last", "No. Hat Tricks" };
            TablePrinter.printResultSet(rs, titles);

            // final int[] SPACINGS = { 16, 15 };
            // printTitles(titles, SPACINGS);
            // printDashes(titles, SPACINGS);

            // String[] columns = new String[titles.length];
            // while (rs.next()) {
            //     columns[0] = rs.getString(1);
            //     columns[1] = rs.getString(2);
            //     columns[2] = rs.getString(3);
            //     printTitles(columns, SPACINGS);
            // }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    /* **********************************************
     *                HELPER METHODS
     ************************************************/

    // box formatting output
    private void printBoxedText(String text) {
        int width = text.length() + 4;
        System.out.println();
        printBorder(width);
        System.out.println("| " + text + " |");
        printBorder(width);
        System.out.println();
    }

    private static void printBorder(int width) {
        for (int i = 0; i < width; i++) {
            System.out.print("-");
        }
        System.out.println();
    }

    // prints each string from titles[i] (in that order) as a formatted row
    // with COL_SPACES[i] amount indentation between each column.
    // The only exception is the last title that gets printed,
    // since it doesnt need indentation because theres noghting after it
    private void printTitles(String[] titles, final int[] COL_SPACES) {
        String title = "";
        for (int i = 0; i < titles.length; i++) {
            if (i < titles.length - 1)
                title += String.format("%-" + COL_SPACES[i] + "s", titles[i]);
            else
                // last output doesnt need indentation
                title += String.format("%s", titles[i]);
        }
        System.out.println(title);
    }

    // same logic as above, but just prints dashes instead
    private void printDashes(String[] titles, final int[] COL_SPACES) {
        String title = "";
        for (int i = 0; i < titles.length; i++) {
            if (i < titles.length - 1)
                // make the indent go as long as the specified width of: COL_SPACES[i]-2
                title += String.format("%-" + COL_SPACES[i] + "s", "-".repeat(COL_SPACES[i] - 2));
            else
                // make this indent go as long as the length of the column name itself
                title += String.format("%s", "-".repeat(titles[i].length()));
        }
        System.out.println(title);
    }

    private String getStat(String line) {
        if (line.equals("g")) {
            return "Goals";
        } else if (line.equals("a")) {
            return "Assists";
        } else if (line.equals("p")) {
            return "Points";
        } else if (line.equals("+")) {
            return "Plus-Minus";
        } else {
            return "unknown";
        }
    }

    private String getStatSQL(String line) {
        if (line.equals("g")) {
            return "numGoals";
        } else if (line.equals("a")) {
            return "numAssists";
        } else if (line.equals("p")) {
            return "numPoints";
        } else if (line.equals("+")) {
            return "plusMinus";
        } else {
            return "numPoints"; // shouldnt happen, but resort to points
        }
    }

    private String getMins(int seconds) {
        seconds %= 60;
        int minutes = (seconds / 60) % 60;
        return minutes + ":" + seconds;
    }

    private boolean playerExists(String first, String last) {
        try {
            String sql = "SELECT * from players WHERE firstName = ? AND lastName = ?;";
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, first);
            pstmt.setString(2, last);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return true;
            } else {
                printBoxedText(String.format("Error: '%s %s' was not found.", first, last));
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
        return false;
    }

    private boolean teamExists(String teamName) {
        try {
            String sql = "SELECT * from teams WHERE teamName = ?;";
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, teamName);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return true;
            } else {
                printBoxedText(String.format("Error: the team '%s' was not found.", teamName));
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
        return false;
    }
}
