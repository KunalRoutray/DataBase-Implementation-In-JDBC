package comm.dbms.lab;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;

public class lab3_task1_extra1 {

    // Embedded Derby database – creates/opens 'university_db' folder
    private static final String DB_URL = "jdbc:derby:university_db;create=true";

    public static void main(String[] args) {

        // Step 1: Setup tables + data
        setupTablesAndData();

        // Step 2: Create the view
        createHighSalaryView();

        // Step 3: Query and display the view
        listHighSalaryEmployees();
    }

    private static void setupTablesAndData() {

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            // Create departments table
            stmt.execute("""
                CREATE TABLE departments123 (
                    dept_id INT PRIMARY KEY,
                    dept_name VARCHAR(50),
                    dept_location VARCHAR(50)
                )
                """);

            // Create employees table
            stmt.execute("""
                CREATE TABLE employees123 (
                    emp_id INT PRIMARY KEY,
                    name VARCHAR(100),
                    salary DECIMAL(10,2),
                    dept_id INT
                )
                """);

            // Insert sample data (only if tables are empty)
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM employees123")) {

                rs.next();

                if (rs.getInt(1) == 0) {

                    stmt.execute("INSERT INTO departments123 VALUES (1, 'Engineering','E_block')");
                    stmt.execute("INSERT INTO departments123 VALUES (2, 'HR','C_block')");
                    stmt.execute("INSERT INTO departments123 VALUES (3, 'Sales','A_block')");

                    stmt.execute("INSERT INTO employees123 VALUES (1, 'John Doe', 120000, 1)");
                    stmt.execute("INSERT INTO employees123 VALUES (2, 'Jane Smith', 65000, 2)");
                    stmt.execute("INSERT INTO employees123 VALUES (3, 'Alice Johnson', 95000, 1)");
                    stmt.execute("INSERT INTO employees123 VALUES (4, 'Bob Brown', 88000, 3)");
                    stmt.execute("INSERT INTO employees123 VALUES (5, 'Carol White', 130000, 3)");
                    stmt.execute("INSERT INTO employees123 VALUES (6, 'kunal', 916000, 1)");
                    stmt.execute("INSERT INTO employees123 VALUES (7, 'roy', 819000, 1)");
                    stmt.execute("INSERT INTO employees123 VALUES (8, 'rout', 7110000, 2)");
                    stmt.execute("INSERT INTO employees123 VALUES (9, 'rout1', 7110000, 1)");

                    System.out.println("Sample tables and data created.");
                }
            }

        } catch (SQLException e) {

            // Ignore if tables already exist
            if (!e.getMessage().contains("already exists")) {
                System.err.println("Error setting up tables: " + e.getMessage());
            }
        }
    }

    private static void createHighSalaryView() {

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            // Drop view if exists
            try {
                stmt.execute("DROP VIEW high_salary_employees123");
                System.out.println("Old view dropped.");
            } catch (SQLException ignored) {
            }

            String createSql = """
                CREATE VIEW high_salary_employees123 AS
                SELECT e.emp_id, e.name, e.salary, d.dept_name, d.dept_location
                FROM employees123 e
                JOIN departments123 d ON e.dept_id = d.dept_id
                WHERE e.salary > 80000
                ORDER BY e.salary DESC
                """;

            stmt.execute(createSql);

            System.out.println("View 'high_salary_employees123' created successfully.");

        } catch (SQLException e) {

            System.err.println("Error creating view: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void listHighSalaryEmployees() {

        String sql = """
            SELECT * FROM high_salary_employees123
            FETCH FIRST 20 ROWS ONLY
            """;

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\nHigh Salary Employees (from VIEW):");

            System.out.printf("%-8s %-25s %-12s %-20s %-20s%n",
                    "ID", "Name", "Salary", "Department", "Location");

            System.out.println("--------------------------------------------------------------------------");

            int count = 0;

            while (rs.next()) {

                count++;

                System.out.printf("%-8d %-25s %-12.2f %-20s %-20s%n",
                        rs.getInt("emp_id"),
                        rs.getString("name"),
                        rs.getDouble("salary"),
                        rs.getString("dept_name"),
                        rs.getString("dept_location"));
            }

            if (count == 0) {
                System.out.println("No employees with salary > 80,000 found.");
            }

        } catch (SQLException e) {

            System.err.println("Error querying view: " + e.getMessage());
            e.printStackTrace();
        }
    }
}










