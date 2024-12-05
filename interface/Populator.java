import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Populator {

    private final static String PATH_TO_CHUNKS = "../populate_data/sql_chunks/";

    public static void repopulateDB(Connection connection, String file_name) {

        try {

            Statement statement = connection.createStatement();
            BufferedReader reader = new BufferedReader(new FileReader(PATH_TO_CHUNKS + file_name));

            connection.setAutoCommit(false);
            StringBuilder queryBuilder = new StringBuilder();
            String line;
            int batchCount = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // skip empty lines
                if (line.isEmpty()) {
                    continue;
                }

                queryBuilder.append(line).append(" ");

                // build up query
                if (line.endsWith(";")) {
                    String query = queryBuilder.toString().trim();
                    queryBuilder.setLength(0); 

                    statement.addBatch(query);
                    batchCount++;

                    // optimal batch size according to oracle
                    if (batchCount >= 50) {
                        statement.executeBatch();
                        batchCount = 0;
                    }

                }
            }

            // remaining queries in the batch
            if (batchCount > 0) {
                statement.executeBatch();
            }

            connection.commit(); 

            reader.close();
        } catch (IOException e) {
            System.err.println("Error reading the SQL file: " + file_name);
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error connecting to the database or executing queries.");
            e.printStackTrace();
        }

    }

}
