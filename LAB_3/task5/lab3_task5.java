package comm.dbms.lab;

import java.sql.*;

public class lab3_task5 {
    private static final String DB_URL = "jdbc:derby:bankdb;create=true";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            System.out.println("✓ Connected to bankdb\n");
            setupDatabase(conn);

            System.out.println("=== BEFORE TRANSACTION 1 (SCENARIO 1) ===");
            showBalances(conn);

            // Scenario 1: Successful transaction
            System.out.println("\n=== SCENARIO 1: Successful Transfer 1 ===");
            transferFunds(conn, 101, 102, 200, false);

            System.out.println("=== AFTER TRANSACTION 1 (SCENARIO 1) ===");
            showBalances(conn);
            
            
            System.out.println("=== BEFORE TRANSACTION 2 (SCENARIO 1) ===");
            showBalances(conn);

            // Scenario 1: Successful transaction
            System.out.println("\n=== SCENARIO 1: Successful Transfer 2 ===");
            transferFunds(conn, 101, 102, 200, false);

            System.out.println("=== AFTER TRANSACTION 2 (SCENARIO 1) ===");
            showBalances(conn);


            // Reset balances
            resetBalances(conn);

            System.out.println("=== BEFORE TRANSACTION (SCENARIO 2) ===");
            showBalances(conn);

            // Scenario 2: Failed transaction (simulated error)
            System.out.println("\n=== SCENARIO 2: Failed Transfer (Simulated Error) ===");
            transferFunds(conn, 101, 102, 200, true);

            System.out.println("=== AFTER TRANSACTION (SCENARIO 2) ===");
            showBalances(conn);

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
    }

    private static void setupDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE Accounts1112 ("
                + "acct_id INT PRIMARY KEY, "
                + "owner VARCHAR(50), "
                + "balance DECIMAL(10,2) NOT NULL CHECK (balance >= 0))");

            // Insert only if empty
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Accounts1112")) {
                rs.next();
                if (rs.getInt(1) == 0) {
                    stmt.execute("INSERT INTO Accounts1112 VALUES (101, 'Alice', 1200.00)");
                    stmt.execute("INSERT INTO Accounts1112 VALUES (102, 'Bob', 450.00)");
                    stmt.execute("INSERT INTO Accounts1112 VALUES (103, 'Charlie', 780.00)");
                    stmt.execute("INSERT INTO Accounts1112 VALUES (104, 'David', 920.00)");
                    stmt.execute("INSERT INTO Accounts1112 VALUES (105, 'Eve', 610.00)");
                    System.out.println("✓ Accounts table created with 5 sample accounts");
                }
            }
        }
    }

    private static void resetBalances(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE Accounts1112 SET balance = 1200.00 WHERE acct_id = 101");
            stmt.execute("UPDATE Accounts1112 SET balance = 450.00 WHERE acct_id = 102");
            stmt.execute("UPDATE Accounts1112 SET balance = 780.00 WHERE acct_id = 103");
            stmt.execute("UPDATE Accounts1112 SET balance = 920.00 WHERE acct_id = 104");
            stmt.execute("UPDATE Accounts1112 SET balance = 610.00 WHERE acct_id = 105");
        }
        System.out.println("✓ Balances reset to initial state");
    }

    private static void transferFunds(Connection conn, int fromAcct, int toAcct,
                                       double amount, boolean simulateFailure) throws SQLException {
        conn.setAutoCommit(false);
        System.out.println("→ Starting transaction (auto-commit = false)");

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                "UPDATE Accounts1112 SET balance = balance - " + amount +
                " WHERE acct_id = " + fromAcct);
            System.out.println("✓ Debited $" + amount + " from Account " + fromAcct);

            if (simulateFailure) {
                System.out.println("✗ SIMULATED FAILURE: Network interruption!");
                throw new SQLException("Simulated network failure during transfer");
            }

            stmt.executeUpdate(
                "UPDATE Accounts1112 SET balance = balance + " + amount +
                " WHERE acct_id = " + toAcct);
            System.out.println("✓ Credited $" + amount + " to Account " + toAcct);

            conn.commit();
            System.out.println("✓ Transaction COMMITTED successfully");

        } catch (SQLException e) {
            conn.rollback();
            System.out.println("✗ Transaction ROLLED BACK due to error: " + e.getMessage());
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private static void showBalances(Connection conn) throws SQLException {
        System.out.println("\n=== CURRENT ACCOUNT BALANCES ===");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT owner, acct_id, balance FROM Accounts1112 ORDER BY acct_id")) {
            while (rs.next()) {
                System.out.printf("%-10s (Acct %d): $%,10.2f%n",
                    rs.getString("owner"),
                    rs.getInt("acct_id"),
                    rs.getDouble("balance"));
            }
        }
    }
}
