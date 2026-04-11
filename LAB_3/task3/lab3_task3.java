package comm.dbms.lab;

import java.util.Scanner;

public class lab3_task3 {



	    public static void main(String[] args) {
	        Scanner sc = new Scanner(System.in);
	        System.out.println("=== Relational Algebra Translator Simulator ===");
	        System.out.println("Enter SQL query (one line, press Enter to finish):");
	        System.out.println("Examples:");
	        System.out.println("  SELECT name, salary FROM employee WHERE salary > 60000");
	        System.out.println("  SELECT name FROM student WHERE major = 'CSE' ORDER BY name");
	        System.out.println("  SELECT s.name, c.title FROM student s JOIN course c ON s.cid = c.cid WHERE s.major = 'IT'");
	        System.out.println("  SELECT dept, AVG(salary) FROM employee GROUP BY dept HAVING AVG(salary) > 70000");
	        System.out.println("------------------------------------------------------------");

	        String sql = sc.nextLine().trim().toUpperCase();

	        if (sql.isEmpty()) {
	            System.out.println("No query entered.");
	            return;
	        }

	        System.out.println("\n=== Your SQL Query ===");
	        System.out.println(sql);

	        System.out.println("\n=== Relational Algebra Translation ===");
	        translateToRelationalAlgebra(sql);
	    }

	    private static void translateToRelationalAlgebra(String sql) {
	        try {
	            // Remove extra spaces and normalize
	            sql = sql.replaceAll("\\s+", " ").trim();

	            // Extract SELECT part
	            if (!sql.startsWith("SELECT ")) {
	                System.out.println("Error: Query must start with SELECT");
	                return;
	            }

	            int fromIndex = sql.indexOf(" FROM ");
	            if (fromIndex == -1) {
	                System.out.println("Error: Missing FROM clause");
	                return;
	            }

	            String projection = sql.substring(7, fromIndex).trim(); // after SELECT

	            // Find WHERE, GROUP BY, HAVING, ORDER BY positions
	            int whereIndex = sql.indexOf(" WHERE ");
	            int groupIndex = sql.indexOf(" GROUP BY ");
	            int havingIndex = sql.indexOf(" HAVING ");
	            int orderIndex = sql.indexOf(" ORDER BY ");

	            // Determine end of FROM clause
	            int fromEnd = (whereIndex > 0 ? whereIndex : 
	                          (groupIndex > 0 ? groupIndex : 
	                          (orderIndex > 0 ? orderIndex : sql.length())));

	            String fromClause = sql.substring(fromIndex + 6, fromEnd).trim();

	            StringBuilder ra = new StringBuilder();

	            // Handle JOIN or simple table
	            if (fromClause.contains(" JOIN ")) {
	                // Basic two-table JOIN support
	                String[] joinParts = fromClause.split("\\s+JOIN\\s+");
	                String left = joinParts[0].trim();
	                String rest = joinParts[1].trim();
	                int onIndex = rest.indexOf(" ON ");
	                if (onIndex == -1) {
	                    System.out.println("Error: JOIN without ON clause not supported");
	                    return;
	                }
	                String right = rest.substring(0, onIndex).trim();
	                String onCondition = rest.substring(onIndex + 4).trim();
	                ra.append(left).append(" ⋈_").append(onCondition).append(" ").append(right);
	            } else {
	                ra.append(fromClause);
	            }

	            // Wrap with WHERE (selection)
	            if (whereIndex > 0) {
	                int whereEnd = (groupIndex > 0 ? groupIndex : 
	                               (havingIndex > 0 ? havingIndex : 
	                               (orderIndex > 0 ? orderIndex : sql.length())));
	                String condition = sql.substring(whereIndex + 7, whereEnd).trim();
	                ra.insert(0, "σ " + condition + " (");
	                ra.append(")");
	            }

	            // Wrap with GROUP BY + HAVING (aggregation)
	            if (groupIndex > 0) {
	                int groupEnd = (havingIndex > 0 ? havingIndex : 
	                               (orderIndex > 0 ? orderIndex : sql.length()));
	                String groupBy = sql.substring(groupIndex + 10, groupEnd).trim();
	                ra.insert(0, "γ " + groupBy + " (");
	                ra.append(")");

	                if (havingIndex > 0) {
	                    int havingEnd = (orderIndex > 0 ? orderIndex : sql.length());
	                    String having = sql.substring(havingIndex + 8, havingEnd).trim();
	                    ra.insert(0, "σ " + having + " (");
	                    ra.append(")");
	                }
	            }

	            // Wrap with ORDER BY (sort)
	            if (orderIndex > 0) {
	                String orderBy = sql.substring(orderIndex + 10).trim();
	                ra.append(" τ ").append(orderBy);
	            }

	            // Final projection
	            System.out.println("π " + projection + " (" + ra + ")");
	            
	        } catch (Exception e) {
	            System.out.println("Error parsing SQL: " + e.getMessage());
	            System.out.println("Try simpler queries or check syntax.");
	        }
	    }
	}
