package com.snowflake.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class SnowflakeTrail {
	public static void main(String[] args) throws Exception
	  {
	    // get connection
	    System.out.println("Create JDBC connection");
	    Connection connection = getConnection();
	    System.out.println("Done creating JDBC connectionn");
		
	    // create statement
	    System.out.println("Create JDBC statement");
	    Statement statement = connection.createStatement();
	    System.out.println("Done creating JDBC statementn");
	    
	    // query the data
	    System.out.println("Query demo");
	    ResultSet resultSet = statement.executeQuery("select * from table_name;");

	
	    // fetch metadata
	    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
	    int columnCount = resultSetMetaData.getColumnCount();

            // To get the column names we do a loop for a number of column
            // count returned above. And please remember a JDBC operation
            // is 1-indexed so every index begin from 1 not 0 as in array.
	    
            ArrayList<String> columns = new ArrayList<>();
            for (int i = 1; i < columnCount; i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            columns.add(columnName);
            }

            // Later we use the collected column names to get the value of
            // the column it self.
            while (resultSet.next()) {
                for (String columnName : columns) {
                    String value = resultSet.getString(columnName);
                    System.out.println(columnName + " = " + value);
                }
            }
	    
	    statement.close();
	  }
	   private static Connection getConnection()
	          throws SQLException
	  {
	    try
	    {
	      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex)
	    {
	     System.err.println("Driver not found");
	    }
	    // build connection properties
	    Properties properties = new Properties();
	    properties.put("user", "");     // replace "" with your username
	    properties.put("password", ""); // replace "" with your password
	    properties.put("account", "");  // replace "" with your account name
	    properties.put("db", "");       // replace "" with target database name
	    properties.put("schema", "");   // replace "" with target schema name

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    
	    String user = "username";          // replace "<user>" with your user name
	    String password = "password";  // replace "<password>" with your password
	    Connection con = DriverManager.getConnection("url", user, password);
	    return con;
	  }
}
