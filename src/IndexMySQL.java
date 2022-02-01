import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Tests creating an index and using EXPLAIN on a MySQL database.
 */
public class IndexMySQL 
{
	/**
	 * Connection to database
	 */
	private Connection con;
	
	
	/**
	 * Main method is only used for convenience.  Use JUnit test file to verify your answer.
	 * 
	 * @param args
	 * 		none expected
	 * @throws SQLException
	 * 		if a database error occurs
	 */
	public static void main(String[] args) throws SQLException
	{
		IndexMySQL q = new IndexMySQL();
		q.connect();	
		q.drop();
		q.create();
		q.insert(10000);	
		q.addindex1();	
		q.addindex2();			
		q.close();
	}

	/**
	 * Makes a connection to the database and returns connection to caller.
	 * 
	 * @return
	 * 		connection
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public Connection connect() throws SQLException
	{
		String url = "jdbc:mysql://localhost/lab2";
		String uid = "testuser";
		String pw = "404testpw";
		
		System.out.println("Connecting to database.");
		// Note: Must assign connection to instance variable as well as returning it back to the caller
		// TODO: Make a connection to the database and store connection in con variable before returning it.

		con = DriverManager.getConnection(url, uid, pw);

		return con;  	                       
	}
	
	/**
	 * Closes connection to database.
	 */
	public void close()
	{
		System.out.println("Closing database connection.");
		// TODO: Close the database connection.  Catch any exception and print out if it occurs.	
		
		try {
			con.close();
		} catch (SQLException e) {
			//TODO: handle exception
			System.out.println(e);
		}
	}
	
	/**
	 * Drops the table from the database.  If table does not exist, error is ignored.
	 */
	public void drop()
	{
		System.out.println("Dropping table bench.");
		// TODO: Drop the table bench.  Catch any exception and print out if it occurs.	

		String sql = "DROP TABLE bench";

		try {
			Statement rst = con.createStatement();
			rst.executeUpdate(sql);
		} catch (SQLException e) {
			//TODO: handle exception
			System.out.println(e);
		}



	}

	
	/**
	 * Creates the table in the database.  Table name: bench
	 * Fields:
	 *  - id - integer, must auto-increment
	 *  - val1 - integer (starts at 1 and each record increases by 1)
	 *  - val2 - integer (val1 % 10)
	 *  - str1 - varchar(20) = "Test"+val1
	 */
	public void create() throws SQLException
	{
		System.out.println("Creating table bench.");
		// TODO: Create the table bench.
		String sql = "CREATE TABLE bench (id int AUTO_INCREMENT, val1 int, val2 int AS  (MOD(val1,10)), str1 varchar(20) AS (CONCAT(\"Test\",val1)), PRIMARY KEY(id));";	
		Statement rst = con.createStatement();
		rst.executeUpdate(sql);
			
		
	}
	
	/**
	 * Inserts the test records in the database.  Must used a PreparedStatement.  
	 * 
	 * Challenge: There are ways to speed this up. Bonus +1 mark for finding a technique.	 
	 */
	public void insert(int numRecords) throws SQLException
	{
		System.out.println("Inserting records.");		
		// TODO: Insert records		
		String sql = "INSERT INTO bench (val1)";

		for (int i = 1; i <= numRecords ; i++) {
			if (i == 1) {
				sql += " VALUES (?)";
				System.out.println(sql);
			} else {
				sql += ",(?)";
			}
		}
		PreparedStatement pstmt = con.prepareStatement(sql);
		
		for (int j = 1; j <= numRecords; j++) 
			pstmt.setInt(j, j);
		
		pstmt.executeUpdate();
	}
	
	/**
	 * Creates a unique index on val1 for bench table.  Returns result of explain.
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet addindex1() throws SQLException
	{
		System.out.println("Building index #1.");
		
		// TODO: Create index
		String sql = "CREATE UNIQUE INDEX idxBenchVal1 ON bench (val1)";
		Statement index1 = con.createStatement();
		index1.executeUpdate(sql);

		// TODO: Do explain with query: SELECT * FROM bench WHERE val1 = 500
		 sql = "EXPLAIN SELECT * FROM bench WHERE val1 = 500";
		return index1.executeQuery(sql);
			
	}
	
	/**
	 * Creates an index on val2 and val1 for bench table.  Returns result of explain.
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet addindex2() throws SQLException
	{
		System.out.println("Building index #2.");
		// TODO: Create index
		String sql = "CREATE INDEX idxBenchVal2Val1 ON bench (val2,val1);";
		Statement index2 = con.createStatement();
		index2.executeUpdate(sql);
		index2.executeUpdate("SET optimizer_switch = \'index_condition_pushdown=on\'");
		// TODO: Do explain with query: SELECT * FROM bench WHERE val2 = 0 and val1 > 100;
		sql = "EXPLAIN SELECT * FROM bench WHERE val2 = 0 and val1 > 100";
		return index2.executeQuery(sql);	
	}
	
	
	/*
	 * Do not change anything below here.
	 */
	/**
     * Converts a ResultSet to a string with a given number of rows displayed.
     * Total rows are determined but only the first few are put into a string.
     * 
     * @param rst
     * 		ResultSet
     * @param maxrows
     * 		maximum number of rows to display
     * @return
     * 		String form of results
     * @throws SQLException
     * 		if a database error occurs
     */    
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException
    {                       
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
                
        while (rst.next()) 
        {
            if (rowCount < maxrows)
            {
                for (int j = 0; j < meta.getColumnCount(); j++) 
                { 
                	Object obj = rst.getObject(j + 1);                	 	                       	                                	
                	buf.append(obj);                    
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");                    
                }
                buf.append('\n');
            }
            rowCount++;
        }            
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }
    
    /**
     * Converts ResultSetMetaData into a string.
     * 
     * @param meta
     * 		 ResultSetMetaData
     * @return
     * 		string form of metadata
     * @throws SQLException
     * 		if a database error occurs
     */
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException
    {
	    StringBuffer buf = new StringBuffer(5000);                                   
	    buf.append(meta.getColumnName(1)+" ("+meta.getColumnLabel(1)+", "+meta.getColumnType(1)+"-"+meta.getColumnTypeName(1)+", "+meta.getColumnDisplaySize(1)+", "+meta.getPrecision(1)+", "+meta.getScale(1)+")");
	    for (int j = 2; j <= meta.getColumnCount(); j++)
	        buf.append(", "+meta.getColumnName(j)+" ("+meta.getColumnLabel(j)+", "+meta.getColumnType(j)+"-"+meta.getColumnTypeName(j)+", "+meta.getColumnDisplaySize(j)+", "+meta.getPrecision(j)+", "+meta.getScale(j)+")");
	    return buf.toString();
    }
}
