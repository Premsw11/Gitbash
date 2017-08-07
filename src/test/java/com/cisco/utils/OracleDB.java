package com.cisco.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;

import com.cisco.DBConnections.GetConnection;

public class OracleDB {

	/*public static void main(String[] args) {
		// TODO Auto-generated method stub

		String query ="select consumption_id,'FEATUREHASH' as entitlemet_type from EB_CLASSIC_ENT_CONSUMPTIONS where smart_account_id=113779 ";
		OracleDB gdoDb=new OracleDB();
		int[] primaryKeys={0};
		Connection oracleConnection=null;
		try {
			oracleConnection = GetConnection.getOracleConnection();
			gdoDb.executeQuery(oracleConnection,query,primaryKeys);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {
				if(oracleConnection!=null)
				oracleConnection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}*/
	
	public Hashtable<String,String[]> executeQuery(Connection oracleConnection, String query, int primaryKeys[]){
		
		try {
			
			Statement statement = oracleConnection.createStatement();
			
			//statement.setFetchSize(10000);
			ResultSet resultSet=statement.executeQuery(query);
			Hashtable<String,String[]> totalRecords = new Hashtable<String,String[]>();
						
			ResultSetMetaData rsmd= resultSet.getMetaData();
			int rowNum=1;
			while(resultSet.next())
			{
				 String[] record = new String[rsmd.getColumnCount()];
				for(int i=0;i<rsmd.getColumnCount();i++)
				{
					record[i]=resultSet.getString(i+1);
				}
				String key="";
			//	System.out.println(primaryKeys);
				key="row_"+rowNum;
				rowNum++;
				//System.out.println("Oracle Key "+ key);
				totalRecords.put(key,record);
			}
	
			return totalRecords;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
public Hashtable<String,String[]> executeQueryLimitedReords(Connection oracleConnection, String query, int primaryKeys[],int minLimit, int maxLimit){
		
		try {
			
			Statement statement = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			
			//statement.setFetchSize(10000);
			ResultSet resultSet=statement.executeQuery(query);
			Hashtable<String,String[]> totalRecords = new Hashtable<String,String[]>();
						
			ResultSetMetaData rsmd= resultSet.getMetaData();
			int rowNum=1;
			resultSet.absolute(minLimit);
			int totalRecordsToFetch =maxLimit-minLimit+1;
			while(resultSet.next() && totalRecordsToFetch-- >0)
			{
				 String[] record = new String[rsmd.getColumnCount()];
				for(int i=0;i<rsmd.getColumnCount();i++)
				{
					record[i]=resultSet.getString(i+1);
				}
				String key="";
			//	System.out.println(primaryKeys);
				key="row_"+rowNum;
				rowNum++;
				//System.out.println("Oracle Key "+ key);
				totalRecords.put(key,record);
			}
	
			return totalRecords;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
public long executeQueryToGetCount(Connection oracleConnection, String query, int primaryKeys[]){
		
		try {
			
			Statement statement = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			
			//statement.setFetchSize(10000);
			ResultSet resultSet=statement.executeQuery(query);
			
			resultSet.last();
			int lastrow =resultSet.getRow();
	
			return lastrow;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

}
