package com.cisco.DBConnections;

import java.sql.Connection;
import java.sql.DriverManager;

import org.json.simple.parser.ParseException;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class GetConnection {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			System.out.println(getOracleConnection());
			System.out.println(getMongoDBConnection(null));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection getOracleConnection() throws Exception {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection connection = null;
		/*connection = DriverManager
				.getConnection(
						"jdbc:oracle:thin:@lnxdb-pts1-vm-125-vip.cisco.com:1540/TS2ESM.cisco.com",
						"EBLIC", "EBL1c$789");*/
		
		/*connection = DriverManager
		.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=ON)(FAILOVER=ON)(ADDRESS="
				+ "(PROTOCOL=TCP)(HOST=lnxdb-pts-vm-236-vip.cisco.com)(PORT=1545))(ADDRESS=(PROTOCOL=TCP)"
				+ "(HOST=lnxdb-pts-vm-237-vip.cisco.com)(PORT=1545)))"
				+ "(CONNECT_DATA=(SERVICE_NAME=TS3ESM.cisco.com)(SERVER=DEDICATED)))","EBLIC","EBL1c$789");*/
		
		/*connection = DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=ON)
		 * (FAILOVER=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=lnxdb-pts1-vm-125-vip.cisco.com)(PORT=1540))
		 * (ADDRESS=(PROTOCOL=TCP)(HOST=lnxdb-pts1-vm-126-vip.cisco.com)(PORT=1540)))
		 * (CONNECT_DATA=(SERVICE_NAME=TS2ESM.cisco.com)(SERVER=DEDICATED)))"
				,"CSWS", "By1V3f1O");*/
		
		/*connection = DriverManager
				.getConnection(
						"jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)"
						+ "(TRANSPORT_CONNECT_TIMEOUT=3)(RETRY_COUNT=1)"
						+ "(ADDRESS_LIST=(FAILOVER=ON)(LOAD_BALANCE=ON)(ADDRESS=(PROTOCOL=tcp)(HOST=173.37.103.78)(PORT=1542))"
						+ "(ADDRESS=(PROTOCOL=tcp)(HOST=173.36.34.198)(PORT=1542))(ADDRESS=(PROTOCOL=tcp)(HOST=173.37.103.79)(PORT=1542))"
						+ "(ADDRESS=(PROTOCOL=tcp)(HOST=173.36.34.199)(PORT=1542)))"
						+ "(CONNECT_DATA=(SERVICE_NAME=ESMPRD.cisco.com)))","EBLICRO", "EBL1c$007");*/
		
		/*Connection con=DriverManager.getConnection(  
				"jdbc:oracle:thin:@localhost:1542:CSSWSTG.cisco.com","SED_READ_ONLY","c$sco23");  */
		
		connection = DriverManager.getConnection(
						"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=OFF)(FAILOVER=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=173.37.234.46)(PORT=1541))(ADDRESS=(PROTOCOL=TCP)(HOST=173.37.234.47)(PORT=1541))(ADDRESS=(PROTOCOL=TCP)(HOST=173.36.14.147)(PORT=1541))(ADDRESS=(PROTOCOL=TCP)(HOST=173.36.14.148)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=CSSWPRD.cisco.com)(SERVER=DEDICATED)))","SED_READ_ONLY", "sed_Re#d0nly");
		
		return connection;
	}
	
	public static Connection getOracleConnectionFromCG1() throws Exception {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection connection = null;
		
		Connection con=DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=ON)(FAILOVER=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=173.38.21.148)(PORT=1841))(ADDRESS=(PROTOCOL=TCP)(HOST=173.38.21.149)(PORT=1841))(ADDRESS=(PROTOCOL=TCP)(HOST=173.38.21.150)(PORT=1841)))(CONNECT_DATA=(SERVICE_NAME=TS3CG1_SRVC_OTH.cisco.com)(SERVER=DEDICATED)))","APPSRO","Lj0V0s6G");  
		
		return connection;
	}

	public static MongoClient getMongoDBConnection(String mongoDb) throws ParseException {
		if(mongoDb==null || mongoDb.equalsIgnoreCase("mdfmap_db")){
		String dbURI = "mongodb://mdfmap_user:mongo123@mngdb-ebf-prd1-001.cisco.com:27048/mdfmap_db";
		MongoClientURI uri = new MongoClientURI(dbURI);
		MongoClient mongoClient = new MongoClient(uri);
		
		return mongoClient;
		}
		else if( mongoDb.equalsIgnoreCase("device_db"))
		{
			String dbURI = "mongodb://device_user:mongo123@mngdb-ebf-stg-01:27048/device_db";
			MongoClientURI uri = new MongoClientURI(dbURI);
			MongoClient mongoClient = new MongoClient(uri);
			return mongoClient;
		}
		else if(mongoDb.equalsIgnoreCase("userrole_db")){
			String dbURI = "mongodb://userrole_user:mongo123@mngdb-ebf-stg-01:27048/device_db";
			MongoClientURI uri = new MongoClientURI(dbURI);
			MongoClient mongoClient = new MongoClient(uri);
			return mongoClient;

		}
		else if(mongoDb.equalsIgnoreCase("entitlement_db")){
			String dbURI = "mongodb://entitlement_user:mongo123@mngdb-ebf-stg-01:27048/entitlement_db";
			MongoClientURI uri = new MongoClientURI(dbURI);
			MongoClient mongoClient = new MongoClient(uri);
			DB db = mongoClient.getDB("entitlement_db");
			return mongoClient;

		}
		else if( mongoDb.equalsIgnoreCase("subscription_db"))
		{
			String dbURI = "mongodb://subscription_user:webex123@mngdb-ebf-stg-01:27048/subscription_db";
			MongoClientURI uri = new MongoClientURI(dbURI);
			MongoClient mongoClient = new MongoClient(uri);
			return mongoClient;
		}
		else 
		return null;

	}

}
