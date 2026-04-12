package comm.dbms.lab;

import java.sql.*;

public class lab3_task6 {
    private static final String DB_URL = "jdbc:derby:bankdb";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            System.out.println("✓ Connected to bankdb\n");
            resetBalances(conn);
            showBalances(conn, "BEFORE TRANSACTION");

            // Start atomic transaction
            conn.setAutoCommit(false);
            System.out.println("\n→ Starting multi-phase transaction");

            try (Statement stmt = conn.createStatement()) {
                // PHASE 1: Debit Alice
                stmt.executeUpdate("UPDATE Accounts1112 SET balance = balance - 150 WHERE acct_id = 101");
                Savepoint sp1 = conn.setSavepoint("after_alice_debit");
                System.out.println("✓ Phase 1 complete: Debited $150 from Alice");
                showBalances(conn, "After Phase 1");

                // PHASE 2: Credit Bob (partial amount)
                stmt.executeUpdate("UPDATE Accounts1112 SET balance = balance + 100 WHERE acct_id = 102");
                Savepoint sp2 = conn.setSavepoint("after_bob_credit");
                System.out.println("✓ Phase 2 complete: Credited $100 to Bob");
                showBalances(conn, "After Phase 2");
                
             // PHASE 3: Credit Bob (partial amount)
                stmt.executeUpdate("UPDATE Accounts1112 SET balance = balance + 100 WHERE acct_id = 102");
                Savepoint sp3 = conn.setSavepoint("after_bob_credit");
                System.out.println("✓ Phase 3 complete: Credited $100 to Bob");
                showBalances(conn, "After Phase 3");

                // PHASE 4: Credit Charlie (SIMULATE FAILURE)
                try {
                    stmt.executeUpdate("UPDATE Accounts1112 SET balance = balance + 50 WHERE acct_id = ");
                } catch (SQLException e) {
                    System.out.println("✗ Phase 4 FAILED: Invalid account (simulated error)");
                    System.out.println("→ Rolling back ONLY Phase 3 to savepoint 'after_bob_credit'");
                    conn.rollback(sp3);
                }

                // Finalize successful phases
                conn.commit();
                System.out.println("\n✓ Transaction COMMITTED with partial success");
                showBalances(conn, "FINAL STATE");
                
            } catch (SQLException e) {
                conn.rollback();
                System.err.println("Unexpected error: " + e.getMessage());
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
    }

    private static void resetBalances(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE Accounts1112 SET balance = 1000 WHERE acct_id = 101");
            stmt.execute("UPDATE Accounts1112 SET balance = 500 WHERE acct_id = 102");
            stmt.execute("UPDATE Accounts1112 SET balance = 750 WHERE acct_id = 103");
        }
    }

    private static void showBalances(Connection conn, String label) throws SQLException {
        System.out.println("\n[" + label + "]");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT owner, balance FROM Accounts1112 WHERE acct_id IN (101,102,103) ORDER BY acct_id")) {
            while (rs.next()) {
                System.out.printf("  %s: $%.2f%n", rs.getString("owner"), rs.getDouble("balance"));
            }
        }
    }
}