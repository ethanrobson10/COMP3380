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
 * 
 * 
 * (11) top25 by goals/assists/points/plusMinus
 * 
 * (13) all Teams
 * 
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
            final int[] SPACINGS = { 6, 15 };
            printTitles(titles, SPACINGS);
            printDashes(titles, SPACINGS);

            String[] columns = new String[titles.length];
            while (resultSet.next()) {
                for (int i = 1; i <= columns.length; i++) {
                    columns[i - 1] = resultSet.getString(i);
                }
                printTitles(columns, SPACINGS);
            }

            resultSet.close();
            statement.close();
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

            printBoxedText(String.format("Top 25 Players ordered by %s", getStat(statType)));
            String[] titles = { "Rank", "First", "Last", "Goals", "Assists", "Points", "Plus Minus" };
            final int[] SPACINGS = { 6, 14, 15, 8, 9, 8 };
            printTitles(titles, SPACINGS);
            printDashes(titles, SPACINGS);

            String[] columns = new String[titles.length];
            int rank = 1;
            while (rs.next()) {
                columns[0] = "" + rank;
                // populate each row before printing it
                for (int i = 1; i < titles.length; i++) {
                    columns[i] = rs.getString(i);
                }
                printTitles(columns, SPACINGS);
                rank++;
            }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    // (1)
    public void totalGoalsByTeam(String first, String last) {
        try {

            String sql = """
                    	SELECT teamName, COUNT(*) as numGoals
                    	FROM teams
                    	JOIN playsOn ON teams.teamID = playsOn.teamID
                    	JOIN plays ON playsOn.playerID = plays.goalieID
                    	JOIN players ON plays.playerID = players.playerID
                    	WHERE
                    	players.firstName = ?
                    	AND players.lastName = ?
                    	AND playType = 'Goal'
                    	GROUP BY teamName  ORDER BY numGoals DESC;
                    """;

            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, first);
            pstmt.setString(2, last);

            ResultSet rs = pstmt.executeQuery();

            if (!rs.next()) {
                printBoxedText(String.format("Error: '%s %s' was not found.", first, last));
            } else {

                printBoxedText(String.format("Goals against each team for %s %s", first, last));

                String[] titles = { "Team Name", "Goals Scored" };
                final int[] SPACINGS = { 16 }; // SPACINGS[[i] is width of i'th column
                printTitles(titles, SPACINGS);
                printDashes(titles, SPACINGS);

                int totalGoals = 0;
                do {
                    String[] columns = { rs.getString("teamName"), rs.getString("numGoals") };
                    printTitles(columns, SPACINGS);
                    totalGoals += rs.getInt("numGoals");
                } while (rs.next());

                // Also display total goals after printing ?
                printDashes(titles, SPACINGS);
                String[] totals = { "Total:", "" + totalGoals };
                printTitles(totals, SPACINGS);
                System.out.println();
            }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }

    }

    // (2)
    public void totalGAP(String first, String last) {
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

            if (!rs.next()) {
                printBoxedText(String.format("Error: '%s %s' was not found.", first, last));
            } else {

                printBoxedText(String.format("Total Goals, Assists, and Points for %s %s", first, last));

                String[] titles = { "Player ID", "Season", "Goals", "Assists", "Points"};
                final int[] SPACINGS = {11, 12, 8, 9}; // SPACINGS[[i] is width of i'th column
                printTitles(titles, SPACINGS);
                printDashes(titles, SPACINGS);

                String[] columns = new String[titles.length];
                do {
                    for (int i = 1; i <= titles.length; i++) {
                        columns[i-1] = rs.getString(i);
                    }
                    printTitles(columns, SPACINGS);
                } while (rs.next());
            }

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
            final int[] SPACINGS = { 12 };
            printTitles(titles, SPACINGS);
            printDashes(titles, SPACINGS);

            String[] columns = new String[titles.length];
            while (rs.next()) {
                columns[0] = rs.getString("playType");
                columns[1] = getMins(rs.getInt("avgShiftLength"));
                printTitles(columns, SPACINGS);
            }

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
            final int[] SPACINGS = { 28 };
            printTitles(titles, SPACINGS);
            printDashes(titles, SPACINGS);

            String[] columns = new String[titles.length];
            while (rs.next()) {
                columns[0] = rs.getString("venueName");
                columns[1] = rs.getString("numGoals");
                printTitles(columns, SPACINGS);
            }

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
            // pstmt.setInt(1, numRows); // Doesn't allow a placeholder parameter next to top see FIX below

            ResultSet rs = pstmt.executeQuery();

            printBoxedText(String.format("Top %d officials who call the most penalites against away teams", numRows));
            String[] titles = { "Rank", "Name", "Penalties Called" };
            final int[] SPACINGS = { 6, 20, 16 };
            printTitles(titles, SPACINGS);
            printDashes(titles, SPACINGS);

            String[] columns = new String[titles.length];
            int rank = 1;
            while (rs.next() && rank <= numRows) { //FIX: print until rank equals desired numRows
                columns[0] = "" + rank;
                // populate each row before printing it
                for (int i = 1; i < titles.length; i++) {
                    columns[i] = rs.getString(i);
                }
                printTitles(columns, SPACINGS);
                rank++;
            }

            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }


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
        int minutes = (seconds/60) % 60;
        return minutes + ":" + seconds;
    }
}
