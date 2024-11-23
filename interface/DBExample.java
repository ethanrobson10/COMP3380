
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.Scanner;
import java.sql.Statement;

public class DBExample {
	static Connection connection;

	public static void main(String[] args) throws Exception {
		MyDatabase db = new MyDatabase();
		runConsole(db);

		System.out.println("Exiting...");
	}


	public static void runConsole(MyDatabase db) {

		Scanner console = new Scanner(System.in);
		welcomeMsg();
		System.out.print("db > ");
		String line = console.nextLine();
		String[] parts;
		String arg = "";

		while (line != null && !line.equals("q")) {
			parts = line.split("\\s+");
			if (line.indexOf(" ") > 0)
				arg = line.substring(line.indexOf(" ")).trim();

			if (parts[0].equals("h"))
				printHelp();
			else if (parts[0].equals("teams")) {
				db.allTeams();
			} else if(parts[0].equals("terms")) {
				printTerms();
			}

			else if (parts[0].equals("tgbt")) { 

				String firstName = "";
				while(firstName.length() == 0){
					System.out.print("\nEnter the players first name: ");
					line = console.nextLine();
					firstName = line;
				}
				
				String lastName = "";
				while (lastName.length() == 0) {
					System.out.print("Enter the players last name: ");
					line = console.nextLine();
					lastName = line;
				}

				db.totalGoalsByTeam(firstName, lastName);


			}

			else if (parts[0].equals("top25")) {
				
				String statType = "";
				while(statType.length() == 0){
					
					System.out.print("\nEnter desired statistic (g/a/p/+): ");
					line = console.nextLine();
					if(line.equals("g")){
						statType = "g";
					} else if(line.equals("a")){
						statType = "a";
					} else if(line.equals("p")){
						statType = "p";
					} else if(line.equals("+")){
						statType = "+";
					} else {
						System.out.println("Sorry Invalid statistic: '" + line + "'");
					}
				}

				String season = "";
				while(season.length() == 0){
					
					System.out.print("\nSelect a season:\n'1' for 2018-2019\n'2' for 2019-2020\nEnter coresponding number: ");
					line = console.nextLine();
					if(line.equals("1")){
						season = "2018-2019";
					} else if(line.equals("2")){
						season = "2018-2019";
					} else {
						System.out.println("Sorry '" + line + "' is not a season option\n");
					}
				}

				db.top25byStat(statType, season);
			}

			else if (parts[0].equals("sell")) {
				try {
					if (parts.length >= 2) {
						//db.lookupWhoSells(arg);
					} else {
						System.out.println("Require an argument for this command");
					}
				} catch (Exception e) {
					System.out.println("id must be an integer");
				}
			}

			else if (parts[0].equals("notsell")) {
				try {
					if (parts.length >= 2){
						//db.whoDoesNotSell(arg);
					} else {
						System.out.println("Require an argument for this command");
					}
				} catch (Exception e) {
					System.out.println("id must be an integer");
				}
			}

			else if (parts[0].equals("mc")) {
				//db.mostCities();
			}

			else if (parts[0].equals("notread")) {
				//db.ownBooks();
			}

			else if (parts[0].equals("all")) {
				//db.readAll();
			}

			else if (parts[0].equals("mr")) {
				//db.mostReadPerCountry();
			}

			else
				System.out.println("Read the help with h, or find help somewhere else.");

			System.out.print("db > ");
			line = console.nextLine();
		}

		console.close();
	}

	private static void welcomeMsg() {
        hockeyStickEnclosedMsg(5, "Welcome to the NHL Database!");

        String msg = """

                Explore detailed statistics of your favourite players and even referees!
                The database covers seasons from 2018-2020.

                Type h for the help menu.
                """;
        System.out.println(msg);
    }

	private static void hockeyStickEnclosedMsg(int stickSize, String message) {
        final int STICK_BLADE_LEN = stickSize;
        int messagePadding = message.length(); // Account for spaces around the message.
    
        // Loop to create the stick blade
        for (int i = 0; i < STICK_BLADE_LEN; i++) {
            StringBuilder str = new StringBuilder();
    
            // Left hockey stick
            str.append(" ".repeat(i)).append("\\\\").append(" ".repeat(STICK_BLADE_LEN-i-1));
    
            // Message or spaces in the middle
            if (i == STICK_BLADE_LEN / 2) {
                str.append(message).append(" ".repeat(STICK_BLADE_LEN-i-1)).append("//");
            } else if (i == STICK_BLADE_LEN - 1) {
                str.append("=".repeat(STICK_BLADE_LEN));
                str.append(" ".repeat(messagePadding-(2*STICK_BLADE_LEN)));
                str.append("=".repeat(STICK_BLADE_LEN)).append("//");
            } else {
                str.append(" ".repeat(messagePadding + STICK_BLADE_LEN-i-1));
                str.append("//");
            }
            System.out.println(str);
        }
    }

	private static void printHelp() {
		System.out.println("====================================================== HELP MENU ====================================================================");
		System.out.println("    COMMAND     |        DESCRIPTION                                 |        PARAMETERS        ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  h             |  Displays this menu                                |  none             ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  q             |  Exits this program                                |  none             ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  ex            |  (For new users) Displays an example of user       |  none             ");
		System.out.println("                |  inputs to find a player and get their statistics  |                   ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  terms         |  (For new users) Lists the meaning of any          |  none             ");
		System.out.println("                |  unfamiliar hockey terms in the system             |                   ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  top25         |  Displays the top 25 players determined by your    |  statistic: 'g'=goals, 'a'=assists, 'p'=points, '+'=plus-minus");
		System.out.println("                |  desired statistic, for a particular season        |  season: regular season to calculate the top player statistics");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  tgbt          |  Displays total goals scored on each team          |  first: first name of the player                              ");	  
		System.out.println("                |  for a chosen player                               |  last: last name of the player                                ");
		System.out.println("=====================================================================================================================================");

	}

	public static void printTerms() {
		System.out.println("=========== TERMINOLOGY ====================================================================");
        System.out.println("      TERM      |        DEFINITION                                                        ");
        System.out.println("----------------+---------------------------------------------------------------------------");
        System.out.println("  goal          | awarded to a player who scores on the opposing team's goalie           ");
        System.out.println("----------------+---------------------------------------------------------------------------");
        System.out.println("  assist        | awarded to the player or players (maximum two) who touch the puck       ");
        System.out.println("                | prior to the goal scorer, provided no defender plays or possesses       ");
        System.out.println("                | the puck in between                                                    ");
        System.out.println("----------------+---------------------------------------------------------------------------");
        System.out.println("  point         | a goal or an assist                                                    ");
        System.out.println("----------------+---------------------------------------------------------------------------");
        System.out.println("  penalty       | when a player violates the rules of a game and as a result sits in      ");
        System.out.println("                | the penalty box for an amount of time determined by the referee,        ");
        System.out.println("                | typically 2 or 5 minutes                                               ");
        System.out.println("----------------+---------------------------------------------------------------------------");
        System.out.println("  shift         | refers to an instance when a player is on the ice without going back    ");
        System.out.println("                | to their team's bench for rest                                         ");
        System.out.println("----------------+---------------------------------------------------------------------------");
        System.out.println("  period        | a hockey game is broken into 3 periods, each being 20 minutes.          ");
        System.out.println("----------------+---------------------------------------------------------------------------");
        System.out.println("  overtime      | when a hockey game extends the number of periods to 4 or more as a      ");
        System.out.println("                | result of a tie at the end of the 3rd                                  ");
        System.out.println("===========================================================================================");
	}


}

class MyDatabase {
	private Connection connection;
	private static final int COLUMN_SPACE = 20;

	public MyDatabase() {
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
 
		if (username == null || password == null){
			System.out.println("Username or password not provided.");
			System.exit(1);
		}
 
		String connectionUrl =
				"jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
				+ "database=cs3380;"
				+ "user=" + username + ";"
				+ "password="+ password +";"
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

	public void allTeams() {
		try {
			
			String sql = """
				SELECT teamID, city, teamName
				FROM teams;
				""";

			PreparedStatement statement = connection.prepareStatement(sql);
			ResultSet resultSet = statement.executeQuery();

			//System.out.println("All teams: \n");
			printBoxedText("All NHL Teams");

			String[]  titles = {"ID", "City", "Team Name"};
			final int[] SPACINGS = {6, 15};
			printTitles(titles, SPACINGS);
			printDashes(titles, SPACINGS);

			// String colName1 = "teamID";
			// String colName2 = "city";
			// String colName3 = "teamName";

			//System.out.printf("%-" + COLUMN_SPACE + "s%-" + COLUMN_SPACE + "s%-" + COLUMN_SPACE + "s\n", colName1, colName2, colName3);
			//System.out.printf("%-" + COLUMN_SPACE + "s%-" + COLUMN_SPACE + "s%-" + COLUMN_SPACE + "s\n", "-".repeat(colName1.length()), "-".repeat(colName2.length()), "-".repeat(colName2.length()));
			String[] columns = new String[titles.length];
			while (resultSet.next()) {
				for (int i = 1; i <= columns.length; i++) {
					columns[i-1] = resultSet.getString(i);
				}
				printTitles(columns, SPACINGS);
				//System.out.printf("%-" + COLUMN_SPACE + "s%-" + COLUMN_SPACE + "s%-" + COLUMN_SPACE + "s\n", resultSet.getString("teamID"), resultSet.getString("city"), resultSet.getString("teamName"));
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void top25byStat(String statType, String season){
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
			String[] titles = {"Rank", "First", "Last", "Goals", "Assists", "Points", "Plus Minus"};
			final int[] SPACINGS = {6, 14, 15, 8, 9, 8};
			printTitles(titles, SPACINGS);
			printDashes(titles, SPACINGS);

			String[] columns = new String[titles.length];
			int rank = 1;
			while(rs.next()){
				columns[0] = ""+rank;
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
				
				String[] titles = {"Team Name", "Goals Scored"};
				final int[] SPACINGS = {15}; // SPACINGS[[i] is width of i'th column
				printTitles(titles, SPACINGS);
				printDashes(titles, SPACINGS);
				
				int totalGoals = 0;
				do {
					String[] columns = {rs.getString("teamName"), rs.getString("numGoals")};
					printTitles(columns, SPACINGS);
					totalGoals += rs.getInt("numGoals");
				} while (rs.next());
				
				// Also display total goals after printing ?
				printDashes(titles, SPACINGS);
				String[] totals = {"Total:", ""+totalGoals};
				printTitles(totals, SPACINGS);
				System.out.println();
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
			if (i < titles.length-1)
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
			if (i < titles.length-1)
				// make the indent go as long as the specified width of: COL_SPACES[i]-2
				title += String.format("%-" + COL_SPACES[i] + "s", "-".repeat(COL_SPACES[i]-2));
			else 	
				// make this indent go as long as the length of the column name itself
				title += String.format("%s", "-".repeat(titles[i].length()));
		}
		System.out.println(title);
	}

	private String getStat(String line) {
		if(line.equals("g")){
			return "Goals";
		} else if(line.equals("a")){
			return "Assists";
		} else if(line.equals("p")){
			return "Points";
		} else if(line.equals("+")){
			return "Plus-Minus";
		} else {
			return "unknown";
		}
	}

	private String getStatSQL(String line) {
		if(line.equals("g")){
			return "numGoals";
		} else if(line.equals("a")){
			return "numAssists";
		} else if(line.equals("p")){
			return "numPoints";
		} else if(line.equals("+")){
			return "plusMinus";
		} else {
			return "numPoints"; // shouldnt happen, but resort to points
		}
	}
}
