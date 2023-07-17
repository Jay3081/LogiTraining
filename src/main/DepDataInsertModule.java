package main;

import java.io.FileInputStream; 
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DepDataInsertModule {

	private static Logger demoLogger = LogManager.getLogger(DepDataInsertModule.class.getName());
//Method to Insert Data into The Table
public void insertData(Connection conn, String table_name, String depname) {
	Statement stmt;
	try {
		String query = String.format("insert into %s(depname) values ('%s');", table_name , depname);
		stmt = conn.createStatement();
		stmt.executeUpdate(query);
		demoLogger.info("Row Inserted");
	} catch (Exception e) {
		System.out.println(e);
	}
}


public static void main(String[] args) {
	
	//Get properties from File
	Properties pr = new Properties();
	 try (InputStream input = new FileInputStream("resources/config.properties")) {
         pr.load(input);
     } catch (IOException e) {
         e.printStackTrace();
     }
	 String dbUrl = pr.getProperty("db.url");
     String dbUsername = pr.getProperty("db.username");
     String dbPassword = pr.getProperty("db.password");
     String table1 = pr.getProperty("db.table1");
	try {
		// Establish the connection
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
		DepDataInsertModule obj[] = new DepDataInsertModule[100];
		int numRecordToInsert = Integer.parseInt(pr.getProperty("db.recordCount"));
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numRecordToInsert; i++)
		{
		obj[i] = new DepDataInsertModule();
		 obj[i].insertData(con, table1,"Development");
		}
		long endTime = System.currentTimeMillis();
		long totalTimeTaken = endTime - startTime;
		demoLogger.info("Total Record inserted are " + numRecordToInsert);
		demoLogger.info("Total Time Taken (ms):" + totalTimeTaken);
		con.close();
	} catch (SQLException | ClassNotFoundException e) {
		e.printStackTrace();
	}
}
}

//	public void createTable(Connection conn , String table_name)
//	{
//		Statement stmt;
//		try
//		{
//			String query  = "create table "+table_name+" (depid SERIAL,depname varchar(200));";
//			stmt = conn.createStatement();
//			stmt.executeUpdate(query);
//			System.out.println("Table Created");
//		}catch(Exception e)
//		{
//			System.out.println(e);
//		}
//	}
//public void readData(Connection conn, String table_name) {
//	Statement stmt;
//	ResultSet rs;
//	try {
//		String query = String.format("select * from %s", table_name);
//		stmt = conn.createStatement();
//		rs = stmt.executeQuery(query);
//		while (rs.next()) {
//			System.out.print(rs.getInt("depid") + " ");
//			System.out.println(rs.getString("depname") + " ");			
//		}
//	} catch (Exception e) {
//		System.out.println(e);
//	}
//}