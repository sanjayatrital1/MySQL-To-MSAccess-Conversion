package MySQLToMSAccess;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import java.awt.List;
import java.sql.*;

public class DBConversion {

	public static void main(String[] args) throws SQLException {
		
		java.sql.Connection mysqlcon = null;
		java.sql.Connection accesscon = null;
		java.sql.Statement st = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			mysqlcon = DriverManager.getConnection("jdbc:mysql://localhost:3306/finalproject", "root", "");
			st = mysqlcon.createStatement();
			
			accesscon = DriverManager.getConnection("jdbc:ucanaccess://c:/Users/sanjay/Documents/finalproject.accdb;memory=false", "root", "");
			
			// mysql table name
			String mysqltable = "billing";
			//msaccess table name
			String msaccesstable = "billing";
				
			 try (java.sql.PreparedStatement s1 = mysqlcon.prepareStatement("select * from "+ mysqltable);
			         ResultSet rs = s1.executeQuery()) {
			        ResultSetMetaData meta = rs.getMetaData();

			        ArrayList<String> columns = new ArrayList<>();
			        for (int i = 1; i <= meta.getColumnCount(); i++)
			            columns.add(meta.getColumnName(i));

			        try (java.sql.PreparedStatement s2 = accesscon.prepareStatement(
			                "INSERT INTO "+ msaccesstable+" ("
			              + columns.stream().collect(Collectors.joining(", "))
			              + ") VALUES ("
			              + columns.stream().map(c -> "?").collect(Collectors.joining(", "))
			              + ")"
			        )) {

			            while (rs.next()) {
			                for (int i = 1; i <= meta.getColumnCount(); i++)
			                    s2.setObject(i, rs.getObject(i));

			                s2.addBatch();
			            }

			            s2.executeBatch();
			        }
			    }
			 
			
			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//mysqlcon.commit();
		//accesscon.commit();
		st.close();
		mysqlcon.close();
		accesscon.close();
		
	}

}
