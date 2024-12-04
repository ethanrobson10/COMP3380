import java.sql.*;
import java.util.ArrayList;
// import java.util.List;
import java.util.Arrays;
import java.util.List;

public class TablePrinter {

    private static int PADDING = 3;

    public static void printResultSet(ResultSet rs, String[] headers) {

        List<List<String>> tableData = getTableData(rs);
        List<Integer> col_spaces = getColumnSpaces(tableData, headers, tableData.size());

        printRow(Arrays.asList(headers), col_spaces);
        printDashes(col_spaces);

        // loop through the data one by one
        for (int i = 0; i < tableData.size(); i++) {
            printRow(tableData.get(i), col_spaces);
        }
    }

    public static void printResultSetWithRank(ResultSet rs, String[] headers, int numRows) {

        List<List<String>> tableData = getTableData(rs);
        List<Integer> col_spaces = getColumnSpaces(tableData, Arrays.copyOfRange(headers, 1, headers.length), numRows);

        int rank = 1;
        int i = 0;
        // add rank to the arrays
        while (i < tableData.size() && i < numRows) {
            tableData.get(i).add(0, "" + rank);
            rank++;
            i++;
        }

        // make rank column width of "Rank"+1
        col_spaces.add(0, headers[0].length() + 1);

        printRow(Arrays.asList(headers), col_spaces);
        printDashes(col_spaces);

        // loop through the data one by one
        i = 0;
        while (i < tableData.size() && i < numRows) {
            printRow(tableData.get(i), col_spaces);
            i++;
        }

    }

    private static List<List<String>> getTableData(ResultSet rs) {

        List<List<String>> tableData = new ArrayList<>();

        try {
            // metadata to tell us how many columns were dealing with
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) { // store result set data into 2D list

                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {

                    row.add(rs.getString(i)); // insert row
                }

                tableData.add(row); // Add the row to the 2D ArrayList
            }
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }

        return tableData;
    }

    private static List<Integer> getColumnSpaces(List<List<String>> tableData, String[] headers, int maxRows) {

        // col_spaces[i] is width of i'th column, set all 0 to start
        List<Integer> col_spaces = new ArrayList<>(headers.length);
        for (int i = 0; i < headers.length; i++) {
            // headers are initially width of titles, unless they need to be longer
            col_spaces.add(headers[i].length());
        }

        int i = 0;
        while (i < maxRows && i < tableData.size()) {
            List<String> curRow = tableData.get(i);

            for (int j = 0; j < curRow.size(); j++) {
                // continuously update the width of the maximum length for column[j]
                int col_max = Math.max(col_spaces.get(j), curRow.get(j).length());
                col_spaces.set(j, col_max);
            }
            i++;
        }

        // for (int i = 0; i < tableData.size(); i++) {
        // }

        return col_spaces;
    }

    private static void printRow(List<String> currRow, List<Integer> col_spaces) {
        String title = "";
        for (int i = 0; i < currRow.size(); i++) {
            int spacing = col_spaces.get(i) + PADDING;
            title += String.format("%-" + spacing + "s", currRow.get(i));
        }
        System.out.println(title);
    }

    private static void printDashes(List<Integer> col_spaces) {
        String title = "";
        for (int i = 0; i < col_spaces.size(); i++) {
            int spacing = col_spaces.get(i) + PADDING;
            title += String.format("%-" + spacing + "s", "-".repeat(col_spaces.get(i)));
        }
        System.out.println(title);
    }

}