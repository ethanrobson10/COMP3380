
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
		System.out.print("Welcome! Type h for help. ");
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
			else if (parts[0].equals("mp")) {
				//db.getMostPublishers();
			}

			else if (parts[0].equals("s")) {
				if (parts.length >= 2){
					//db.nameSearch(arg);
				} else {
					System.out.println("Require an argument for this command");
				}
			}

			else if (parts[0].equals("l")) {
				try {
					if (parts.length >= 2){
						//db.lookupByID(arg);
					} else {
						System.out.println("Require an argument for this command");
					}
				} catch (Exception e) {
					System.out.println("id must be an integer");
				}
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

	private static void printHelp() {
		System.out.println("Library database");
		System.out.println("Commands:");
		System.out.println("h - Get help");
		System.out.println("s <name> - Search for a name");
		System.out.println("l <id> - Search for a user by id");
		System.out.println("sell <author id> - Search for a stores that sell books by this id");
		System.out.println("notread - Books not read by its own author");
		System.out.println("all - Authors that have read all their own books");
		System.out.println("notsell <author id>  - list of stores that do not sell this author");
		System.out.println("mp - Authors with the most publishers");
		System.out.println("mc - Authors with books in the most cities");
		System.out.println("mr - Most read book by country");
		System.out.println("");

		System.out.println("q - Exit the program");

		System.out.println("---- end help ----- ");
	}


}

class MyDatabase {
	private Connection connection;

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
}
