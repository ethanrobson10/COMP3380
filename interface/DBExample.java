
import java.util.Scanner;

public class DBExample {

	private static final String[] SEASONS = {"2012-2013", "2013-2014", "2014-2015", "2015-2016", "2016-2017", "2017-2018", "2018-2019", "2019-2020"};

	public static void main(String[] args) throws Exception {
		
		HockeyDB db = new HockeyDB();
		runConsole(db);
		
		System.out.println("Exiting...");
	}


	public static void runConsole(HockeyDB db) {

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

				String firstName = getTextInput(console, "\nEnter the players first name: " );
				String lastName = getTextInput(console, "\nEnter the players last name: " );

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

				String season = getSeason(console);
				
				db.top25byStat(statType, season);
			}

			else if (parts[0].equals("tgap")) {
				String firstName = getTextInput(console, "\nEnter the players first name: " );
				String lastName = getTextInput(console, "\nEnter the players last name: " );
				db.totalGAP(firstName, lastName);
			}

			else if (parts[0].equals("asl")) {
				db.avgShiftByPlay();
			}

			else if (parts[0].equals("gba")) {
				String season = getSeason(console);
				db.goalsByVenue(season);
			}

			else if (parts[0].equals("topNO")) { //Top N Officials
				int numRows = getValidInt("refs", console); // "refs" is the type we want to list
				db.topNOfficialPenalties(numRows);
			}

			else if (parts[0].equals("mostTeams")) {
				int numRows = getValidInt("players", console); // "players" is the type we want to list
				db.topTeamsPlayedFor(numRows);
			}

			else if (parts[0].equals("aslp")) {
				db.avgShiftLengthByPeriod();
			}

			else if (parts[0].equals("pw")) {
				String teamName = getTextInput(console, "\nEnter the team name: ");
				String season = getSeason(console);
				
				db.totalPlayoffWins(teamName, season);
			} 

			else if(parts[0].equals("s")) {
				String teamName = getTextInput(console, "\nEnter the team name: ");
				String season = getSeason(console);
				
				db.schedule(teamName, season);
			}

			else if(parts[0].equals("tpp")) {
				int numRows = getValidInt("players", console); // "refs" is the type we want to list
				db.topPlayersPenalties(numRows);
			}

			else if(parts[0].equals("psAll")) {
				db.playersScoredAgainstAllTeams();
			}

			else if(parts[0].equals("gps")) {
				db.goalsPerShotAllPlayers();
			}

			else if (parts[0].equals("sp")) { 

				String name = getTextInput(console, "\nEnter a player name (first, last, or both): ");
				db.searchPlayer(name);
			} 

			else if(parts[0].equals("ex")) {
				db.example();
			}

			else
				System.out.println("Unknown command, type h for help.");

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
		System.out.println("  tgbt          |  Displays total goals scored against each team     |  first: first name of the player                              ");	  
		System.out.println("                |  for a chosen player                               |  last: last name of the player                                ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  asl           |  View the average shift length for a player before |  none             ");	  
		System.out.println("                |  they attain one of the possible play types        |                   ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  gba           |  List the total goals scored at each NHL           |  season: the hockey season used for the calculation           ");	  
		System.out.println("                |  arena for a particular season                     |                   ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  tgap          |  Displays a players goals, assists, and            |  first: first name of the player                              ");	  
		System.out.println("                |  total points from each season                     |  last: last name of the player                                ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  topNO         |  Displays the top 'numRows' officials that call    |  numRows: the number of officials to display                  ");	  
		System.out.println("                |  the most penalties against away teams             |                                ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  tpp           |  Displays the top 'numRows' players that have      |  numRows: the number of players to display                  ");	  
		System.out.println("                |  taken the most penalties                          |                                ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  aslp          |  average shift length per period                   |  none");	  
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  mostTeams     |  Displays the top 'numRows' players who have       |  numRows: the number of players to display                  ");	  
		System.out.println("                |  played for the most NHL teams                     |                                ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  pw            |  displays the total play off wins for a team       |  team: team to display wins for                   ");	  
		System.out.println("                |  in a particular season                            |  season: the hockey season used for the calculations           ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  psAll         |  displays all players who have scored against      |  none                  ");	  
		System.out.println("                |  all teams (not including their current team)      |                        ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  gps           |  displays all players and their goals per shot     |  none                  ");	  
		System.out.println("                |  across their entire career (in descending order)  |                        ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  sp            |  displays all players that have a matching first   |  name: first name or last name of a player                   ");	  
		System.out.println("                |  or last name as the entered 'name'                |        (can be a partial match)                ");
		System.out.println("----------------+----------------------------------------------------+---------------------------------------------------------------");
		System.out.println("  s             |  displays a teams schedule for a particular        |  team: team to display schedule for                   ");	  
		System.out.println("                |  regular season                                    |  season: the season to find the schedule for          ");
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

// new get Season method just adjust global constant SEASONS for correct seasons
	private static String getSeason(Scanner console) {
		String season = "";

		while(season.length() == 0){
			printSeasonPrompt();
			season = validateSeasonNumber(console);
		}

		return season;
	}
	
	// seaons helper method
	private static void printSeasonPrompt(){
		String prompt = "Select a season:\n";

		int count = 1;
		for(int i = 0; i < SEASONS.length; i++){
			prompt += "'" + count + "' for " + SEASONS[i] + "\n";
			count++;
		}
		prompt += "Enter coresponding number: ";

		System.out.print(prompt);
	}

	// seasons helper method
	private static String validateSeasonNumber(Scanner console){
		String season = "";
		String strNum = "";
		
		strNum = console.nextLine();
		for(int i = 0; i < SEASONS.length; i++){
			
			if(strNum.equals("" + (i+1))){
				season = SEASONS[i];
			}
		}
		if(season.length() == 0){
			System.out.println("Sorry, '" + strNum + "' is not a season option");
		}

		return season;
	}


	private static String getTextInput(Scanner console, String prompt) {
		String name = "";
		while(name.length() == 0){
			System.out.print(prompt);
			name = console.nextLine();
		}

		return name;
	}

	private static int getValidInt(String type, Scanner console) {
		int n = 0;
		String line = "";
		
		while(n <= 0) {
			System.out.printf("\nEnter the number of %s to include: ", type);
			line = console.nextLine();
			try {
				n = Integer.parseInt(line);
				if(n < 1) {
					System.out.println("Sorry, integer must be 1 or greater");
				}
			} catch(NumberFormatException nfe) {
				System.out.println("Sorry, '" + n + "' is not a valid integer");
			}
		}

		return n;
	}




}