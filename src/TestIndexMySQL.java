import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests if table and indexes were created correctly. 
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestIndexMySQL 
{
	/**
	 * Class being tested
	 */
	private static IndexMySQL q;
	
	/**
	 * Connection to the database
	 */
	private static Connection con;
	
	/**
	 * Requests a connection to the database.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@BeforeAll
	public static void init() throws Exception 
	{		
		q = new IndexMySQL();
		con = q.connect();					
	}
	
	/**
	 * Closes connection.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@AfterAll
	public static void end() throws Exception 
	{
		q.close();
        
	}
	
	/**
     * Tests drop command.
     */
    @Test
	@Order(1)
    public void testDrop() 
    {    
    	q.drop();
    	
    	// See if table exists
    	try
    	{
	    	Statement stmt = con.createStatement();
	    	stmt.executeQuery("SELECT * FROM bench");
	    	fail("Table player exists and should be dropped!");
    	}
    	catch (SQLException e)
    	{
    		System.out.println(e);
    	}
    }
    
    /**
     * Tests create.
     */
    @Test
	@Order(2)  
    public void testCreate() throws SQLException
    {   
    	q.drop();
    	q.create();
    	
    	// See if table exists
    	try
    	{
	    	Statement stmt = con.createStatement();
	    	ResultSet rst = stmt.executeQuery("SELECT * FROM bench");	    	
	    	
	    	// Verify its metadata
	    	ResultSetMetaData rsmd = rst.getMetaData();
	    	String st = IndexMySQL.resultSetMetaDataToString(rsmd);
	    	System.out.println(st);	    			
	    	assertEquals("id (id, 4-INT, 10, 10, 0), val1 (val1, 4-INT, 10, 10, 0), val2 (val2, 4-INT, 10, 10, 0), str1 (str1, 12-VARCHAR, 20, 20, 0)", st);	    	
    	}
    	catch (SQLException e)
    	{
    		System.out.println(e);
    		fail("Table bench does not exist!");
    	}
    }
    
    /**
     * Tests insert.
     */
    @Test
	@Order(3)  
    public void testInsert() throws SQLException
    {    
    	int numRecords = 10000;
    	long startTime = System.currentTimeMillis();
    	q.insert(numRecords);
    	long time = System.currentTimeMillis() - startTime;
    	
    	System.out.println("Time to perform insert: "+time+" ms.");
    	    	
    	// Verify data was inserted properly
    	startTime = System.currentTimeMillis();
    	Statement stmt = con.createStatement();
	    ResultSet rst = stmt.executeQuery("SELECT * FROM bench");
	    int row = 0;
	    while (rst.next())
	    {	row++;
	    	int id = rst.getInt(1);
	    	int val1 = rst.getInt(2);
	    	int val2 = rst.getInt(3);
	    	String str1 = rst.getString(4);
	    	assertEquals(row, id);
	    	assertEquals(row, val1);
	    	assertEquals(row%10, val2);
	    	assertEquals("Test"+val1, str1);
	    }
	    
	    assertEquals(numRecords, row);
	    
	    time = System.currentTimeMillis() - startTime;    	
    	System.out.println("Time to scan all data: "+time+" ms.");    	    	
    }
    
    /**
     * Tests adding first index.
     */
    @Test
	@Order(4)  
    public void testAddindex1() throws SQLException
    {       
    	String sql = "SELECT * FROM bench WHERE val1 = 5000";
    	
    	// DROP INDEX if existed before
    	try
    	{
    		Statement stmt = con.createStatement();
 	    	stmt.executeUpdate("DROP INDEX idxBenchVal1 ON bench");	  
    	}
    	catch (SQLException e)
    	{}
    
    	// Time before created index
    	timeQuery(sql, "Time to perform query before index: ");
    	    	
    	// Create index
    	ResultSet rst = q.addindex1();		
    	String queryResult = IndexMySQL.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	
    	String answer = "Total columns: 12"
    			+"\nid, select_type, table, partitions, type, possible_keys, key, key_len, ref, rows, filtered, Extra"
    			+"\n1, SIMPLE, bench, null, const, idxBenchVal1, idxBenchVal1, 5, const, 1, 100.0, null" 
    			+"\nTotal results: 1";
				
    	assertEquals(answer, queryResult);
    	
    	// Result if no index: 1, SIMPLE, bench, ALL, null, null, null, null, 10284, Using where
    	
    	// Time after created index
    	timeQuery(sql, "Time to perform query after index: ");    	
    }
    
    /**
     * Tests adding second index.
     */
    @Test
	@Order(5)  
    public void testAddindex2() throws SQLException
    {     	
    	String sql = "SELECT * FROM bench WHERE val2 = 0 and val1 > 100;";
    	
    	// DROP INDEX if existed before
    	try
    	{
    		Statement stmt = con.createStatement();
 	    	stmt.executeUpdate("DROP INDEX idxBenchVal2Val1 ON bench");	  
    	}
    	catch (SQLException e)
    	{}
    	
    	// Time before created index
    	timeQuery(sql, "Time to perform query before index: ");
    	
    	// Create index
    	ResultSet rst = q.addindex2();    	
    	
    	String queryResult = IndexMySQL.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	
    	String answer = "Total columns: 12"
    			+"\nid, select_type, table, partitions, type, possible_keys, key, key_len, ref, rows, filtered, Extra"
    			+"\n1, SIMPLE, bench, null, range, idxBenchVal1,idxBenchVal2Val1, idxBenchVal2Val1, 10, null, 990, 100.0, Using index condition"  // Used to be: Using where
    			+"\nTotal results: 1";
				
    	assertEquals(answer, queryResult);
    	
    	// Result if no index: 1, SIMPLE, bench, ALL, null, null, null, null, 10284, Using where
    	// Result if index only on val1: 1, SIMPLE, bench, const, idxBenchVal1, idxBenchVal1, 5, const, 1, 
    	
    	// Time after created index
    	timeQuery(sql, "Time to perform query after index: ");    	    
    }
    
    /**
     * Time a query and print out time and a message.
     * 
     * @param sql
     * 		query to execute
     * @param message
     * 		message printed with time
     */
    public static void timeQuery(String sql, String message) throws SQLException
    {
    	long startTime = System.currentTimeMillis();
    	Statement stmt = con.createStatement();
		stmt.executeQuery(sql);
    	long time = System.currentTimeMillis() - startTime;    	
    	System.out.println(message+time+" ms.");
    }
    
    /**
     * Runs an SQL query and compares answer to expected answer.  
     * 
     * @param sql
     * 		SQL query
     * @param answer
     * 		expected answer          
     */
    public static void runSQLQuery(String sql, String answer)
    {    	 
         try
         {
        	Statement stmt = con.createStatement();
 	    	ResultSet rst = stmt.executeQuery(sql);	    	
 	    	
 	    	String st = IndexMySQL.resultSetToString(rst, 1000);
 	    	System.out.println(st);	    			
 	    		
 	    	assertEquals(answer, st);	           	             
            
 	    	stmt.close();
         }            
         catch (SQLException e)
         {	
        	 System.out.println(e);
        	 fail("Incorrect exception: "+e);
         }              
    }
}
