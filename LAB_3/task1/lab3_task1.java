
package comm.dbms.lab;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;

public class lab3_task1 {
	
			    private static final String DB_URL = "jdbc:derby:university_db;create=true";

		    public static void main(String[] args) {
		        try (Connection conn = DriverManager.getConnection(DB_URL)) {
		            System.out.println("Connected to Derby embedded database.");

		            // Enable runtime statistics
		            try (CallableStatement cs = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)")) {
		                cs.execute();
		            }

		            // Setup: create tables + sample data if needed
		            setupDatabase(conn);

		            String query = """
		                SELECT s.name, c.title, e.grade
		                FROM student6 s
		                JOIN enrollment6 e ON s.sid = e.student_id
		                JOIN course6 c ON e.course_id = c.cid
		                WHERE s.major = 'CSE'
		                  AND e.semester = 'Spring 2025'
		                ORDER BY s.name
		                """;

		            // BEFORE indexes
		            System.out.println("\n=== BEFORE INDEXES ===");
		            runAndShowPlan(conn, query);

		            // Create indexes
		            createIndexes(conn);

		            // AFTER indexes
		            System.out.println("\n=== AFTER INDEXES ===");
		            runAndShowPlan(conn, query);

		        } catch (SQLException e) {
		            System.err.println("Error: " + e.getMessage());
		            e.printStackTrace();
		        }
		    }

		    private static void setupDatabase(Connection conn) throws SQLException {
		        try (Statement stmt = conn.createStatement()) {
		            // Create tables (safe to re-run)
		            stmt.execute("CREATE TABLE student6 (sid INT PRIMARY KEY, name VARCHAR(100), major VARCHAR(10))");
		            stmt.execute("CREATE TABLE course6 (cid INT PRIMARY KEY, title VARCHAR(100))");
		            stmt.execute("CREATE TABLE enrollment6 (student_id INT, course_id INT, semester VARCHAR(20), grade DECIMAL(4,2))");

		            // Insert sample data only if empty
		            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM student6")) {
		                rs.next();
		                if (rs.getInt(1) == 0) {
		                    stmt.execute("INSERT INTO student6 VALUES (1,'Amit','CSE'), (2,'Priya','IT'), (3,'Rahul','CSE')");
		                    stmt.execute("INSERT INTO course6 VALUES (101,'DBMS'), (102,'Java')");
		                    stmt.execute("INSERT INTO enrollment6 VALUES (1,101,'Spring 2025',8.5), (3,101,'Spring 2025',7.8), (1,102,'Fall 2024',9.0)");
		                    System.out.println("Sample data created.");
		                }
		            }
		        }
		    }

		    private static void createIndexes(Connection conn) throws SQLException {
		        try (Statement stmt = conn.createStatement()) {
		            stmt.execute("CREATE INDEX idx_major ON student6(major)");
		            stmt.execute("CREATE INDEX idx_semester ON enrollment6(semester)");
		            System.out.println("Indexes created.");
		        }
		    }

		    private static void runAndShowPlan(Connection conn, String query) throws SQLException {
		        Instant start = Instant.now();

		        // Run the query (to collect real stats)
		        try (Statement stmt = conn.createStatement();
		             ResultSet rs = stmt.executeQuery(query)) {
		            while (rs.next()) { /* consume */ }
		        }

		        // Get runtime statistics
		        try (CallableStatement cs = conn.prepareCall("VALUES SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()")) {
		            try (ResultSet rs = cs.executeQuery()) {
		                System.out.println("=== Execution Plan Summary ===");
		                while (rs.next()) {
		                    String stats = rs.getString(1);
		                    // Print only key lines (clean output)
		                    if (stats.contains("optimizer estimated") || 
		                        stats.contains("Scan") || 
		                        stats.contains("Join") || 
		                        stats.contains("Sort") || 
		                        stats.contains("Rows")) {
		                        System.out.println(stats.trim());
		                    }
		                }
		            }
		        }

		        long timeMs = Duration.between(start, Instant.now()).toMillis();
		        System.out.println("Time: " + timeMs + " ms");
		        System.out.println("Full details → check 'derby.log' file\n");
		    }
		}