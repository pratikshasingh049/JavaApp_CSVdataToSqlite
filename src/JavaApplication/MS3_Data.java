package JavaApplication;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MS3_Data {
    private Connection connect() {
    	String url = "jdbc:sqlite:C:/Users/prati/mydb.db";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return connection;
    }
    
    public void insert(String A, String B, String C, String D, String E, String F, String G, String H, String I, String J) {
        String sql = "INSERT INTO X(A,B,C,D,E,F,G,H,I,J) VALUES(?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, A);
            pstmt.setString(2, B);
            pstmt.setString(3, C);
            pstmt.setString(4, D);
            pstmt.setString(5, E);
            pstmt.setString(6, F);
            pstmt.setString(7, G);
            pstmt.setString(8, H);
            pstmt.setString(9, I);
            pstmt.setString(10, J);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public static List<String> readFile(String filename) {

        List<String> records = new ArrayList<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    records.add(line);

                }
            }
            return records;
        } catch (IOException e) {
            System.err.format("Exception occurred trying to read '%s'.", filename);
            return null;
        }

    }
    
    public static void writeFile(String filename, List<String> l) throws FileNotFoundException, UnsupportedEncodingException {

        try ( //write to a file
                PrintWriter writer = new PrintWriter(filename, "UTF-8")) {
            for (int i = 0; i < l.size(); i++) {
                writer.println(l.get(i));
            }

            writer.close();
        }

    }
    
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

        //Records that do not match the column count must be written to the bad-data-<timestamp>.csv file
        ArrayList<String> badData = new ArrayList<>();
        ArrayList<String> logData = new ArrayList<>();

        int numberOfRecordsReceived = 0;
        int numberOfRecordsSuccessful = 0;
        int numberOfRecordsFailed = 0;

        MS3_Data m_Data = new MS3_Data();

        String filename = "ms3Interview.csv";
        List<String> testData = readFile(filename);
        System.out.println("Processing...");

        for (int i = 1; i < testData.size(); i++) {
            numberOfRecordsReceived += 1;
            String[] split = (testData.get(i)).split(",");

            if (split.length == 11) {
                m_Data.insert(split[0], split[1], split[2], split[3], split[4] + "\"" + split[5] + "\"", split[6], split[7], split[8], split[9], split[10]);
                numberOfRecordsSuccessful += 1;
            } else {
                badData.add(testData.get(i));
                numberOfRecordsFailed += 1;
            }

        }
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        String badFileName = "bad-data-" + fileSuffix + ".csv";

        writeFile(badFileName, badData);

        logData.add("a. # of records received " + numberOfRecordsReceived);
        logData.add("b. # of records successful " + numberOfRecordsSuccessful);
        logData.add("c. # of records failed " + numberOfRecordsFailed);

        writeFile("log.txt", logData);
        System.out.println("Done..");

    }


}
