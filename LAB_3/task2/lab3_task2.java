package comm.dbms.lab;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;

public class lab3_task2 {


				    private static final String DB_URL = "jdbc:derby:university_db;create=true";

				    public static void main(String[] args) {
				        try (Connection conn = DriverManager.getConnection(DB_URL)) {
				            System.out.println("Connected to Derby embedded database.");

				            // Setup: create employee table + sample data if needed
				            setupEmployeeTable(conn);

				            String baseQuery = "SELECT name, salary FROM employee123 WHERE salary > ? AND dept = ?";

				            final int ITERATIONS = 200;

				            // --- Using Statement (re-parsing every time) ---
				            long timeStatement = runWithStatement(conn, baseQuery, ITERATIONS);
				            System.out.printf("Statement (re-parsed %d times): %d ms%n", ITERATIONS, timeStatement);

				            // --- Using PreparedStatement (cached plan) ---
				            long timePrepared = runWithPreparedStatement(conn, baseQuery, ITERATIONS);
				            System.out.printf("PreparedStatement (cached plan): %d ms%n", timePrepared);

				        } catch (SQLException e) {
				            System.err.println("Error: " + e.getMessage());
				            e.printStackTrace();
				        }
				    }

				    private static void setupEmployeeTable(Connection conn) throws SQLException {
				        try (Statement stmt = conn.createStatement()) {
				            // Create table if not exists
				            stmt.execute("CREATE TABLE employee123 (emp_id INT PRIMARY KEY, name VARCHAR(100), salary DECIMAL(10,2), dept VARCHAR(10))");

				            // Insert sample data only if empty
				            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM employee123")) {
				                rs.next();
				                if (rs.getInt(1) == 0) {
				                    stmt.execute("INSERT INTO employee123 VALUES (1, 'Amit', 85000, 'IT')");
				                    stmt.execute("INSERT INTO employee123 VALUES (2, 'Priya', 72000, 'CSE')");
				                    stmt.execute("INSERT INTO employee123 VALUES (3, 'Rahul', 92000, 'IT')");
				                    stmt.execute("INSERT INTO employee123 VALUES (4, 'Sneha', 68000, 'CSE')");
				                    System.out.println("Sample employee123 data created.");
				                }
				            }
				        }
				    }

				    private static long runWithStatement(Connection conn, String baseQuery, int iterations) throws SQLException {
				        Instant start = Instant.now();

				        for (int i = 0; i < iterations; i++) {
				            double salaryThreshold = 50000 + (i * 100); // vary parameter slightly
				            String dept = (i % 2 == 0) ? "IT" : "CSE";

				            // Re-build SQL string each time → forces re-parsing
				            String sql = String.format("SELECT name, salary FROM employee123 WHERE salary > %.2f AND dept = '%s'",
				                    salaryThreshold, dept);

				            try (Statement stmt = conn.createStatement();
				                 ResultSet rs = stmt.executeQuery(sql)) {
				                while (rs.next()) { /* consume results */ }
				            }
				        }

				        return Duration.between(start, Instant.now()).toMillis();
				    }

				    private static long runWithPreparedStatement(Connection conn, String baseQuery, int iterations) throws SQLException {
				        Instant start = Instant.now();

				        // Prepare once → plan should be cached
				        try (PreparedStatement pstmt = conn.prepareStatement(baseQuery)) {
				            for (int i = 0; i < iterations; i++) {
				                double salaryThreshold = 50000 + (i * 100);
				                String dept = (i % 2 == 0) ? "IT" : "CSE";

				                pstmt.setDouble(1, salaryThreshold);
				                pstmt.setString(2, dept);

				                try (ResultSet rs = pstmt.executeQuery()) {
				                    while (rs.next()) { /* consume results */ }
				                }
				            }
				        }

				        return Duration.between(start, Instant.now()).toMillis();
				    }
				}