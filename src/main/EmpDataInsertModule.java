package main;

//libraries import 
import java.io.FileInputStream; 
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import org.apache.logging.log4j.*;

public class EmpDataInsertModule {
	
	private static Logger demoLogger = LogManager.getLogger(EmpDataInsertModule.class.getName());
	//To insert Data into Table
	public void insertData(Connection conn, String table_name, String name, String address, String depid) {
		Statement stmt;
		try {
			String query = String.format("insert into %s(name,address,depid) values ('%s','%s','%s');", table_name,
					name, address, depid);
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
			demoLogger.info("Row Inserted");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void main(String[] args) {

		Scanner sc = new Scanner(System.in);
		Properties pr = new Properties();
		try (InputStream input = new FileInputStream("resources/config.properties")) {
			pr.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Get data from Properties 
		String dbUrl = pr.getProperty("db.url");
		String dbUsername = pr.getProperty("db.username");
		String dbPassword = pr.getProperty("db.password");
		String table2 = pr.getProperty("db.table2");
		int numRecordToInsert = Integer.parseInt(pr.getProperty("db.recordCount"));

		// Establish the connection
		try {
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
			EmpDataInsertModule obj[] = new EmpDataInsertModule[100];
			long startTime = System.currentTimeMillis();
			for (int i = 0; i < numRecordToInsert; i++)
			{
				obj[i] = new EmpDataInsertModule();
				obj[i].insertData(con, table2, "ex", "ad4", "5");
			}
			long endTime = System.currentTimeMillis();
			long totalTimeTaken = endTime - startTime; 
			demoLogger.info("Total Record inserted are " + numRecordToInsert);
			demoLogger.info("Total Time Taken (ms):" + totalTimeTaken);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
// To create table by your own
//		public void createTable(Connection conn , String table_name)
//		{
//			Statement stmt;
//			try
//			{
//				String query  = "create table "+table_name+" (empid SERIAL,name varchar(200),address varchar(200),primary key(empid) ,depid int references departments(depid));";
//				stmt = conn.createStatement();
//				stmt.executeUpdate(query);
//				System.out.println("Table Created");
//			}catch(Exception e)
//			{
//				System.out.println(e);
//			}
//		}

// Method to Read Data From Database Table
//	public void readData(Connection conn, String table_name) {
//		Statement stmt;
//		ResultSet rs;
//		try {
//			String query = String.format("select * from %s", table_name);
//			stmt = conn.createStatement();
//			rs = stmt.executeQuery(query);
//			while (rs.next()) {
//				System.out.print(rs.getString("empid") + " ");
//				System.out.print(rs.getString("name") + " ");
//				System.out.print(rs.getString("address") + " ");
//				System.out.println(rs.getString("depid") + " ");
//
//			}
//		} catch (Exception e) {
//			System.out.println(e);
//		}
//	}