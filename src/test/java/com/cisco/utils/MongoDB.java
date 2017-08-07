package com.cisco.utils;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDB {
	

	public static MongoCollection<Document> getEntitlementsCollection(){
		List<MongoCredential> mongocred2 = new ArrayList<MongoCredential>();
		List<ServerAddress> seeds2 = new ArrayList<ServerAddress>();

		seeds2.add( new ServerAddress( "mngdb-ebf-stg-01.cisco.com" ,27048));
		
		
		mongocred2.add(MongoCredential.createScramSha1Credential("entitlement_user", 
				"entitlement_db", "mongo123".toCharArray()));

		MongoDatabase database=null;

		MongoClient mongoClient2 = new MongoClient( seeds2,mongocred2);
		MongoCollection<Document> mongoTable2 = null;
		try{
			database = mongoClient2.getDatabase("entitlement_db");
			mongoTable2  =   database.getCollection("entitlements");
			return mongoTable2;
		}catch(Exception e){}
		return null;
	}
	
	public static MongoCollection<Document> getmdfMappingsSyncTable(){
		List<MongoCredential> mongocred = new ArrayList<MongoCredential>();
		List<ServerAddress> seeds = new ArrayList<ServerAddress>();

//		seeds.add( new ServerAddress( "mngdb-ebf-prd1-001.cisco.com" ,27048));
//		mongocred.add(MongoCredential.createScramSha1Credential("mdfmap_user", 
//				"mdfmap_db", "mongo123".toCharArray()));
	
				seeds.add( new ServerAddress( "mngdb-ebf-poe-02.cisco.com" ,27048));
				mongocred.add(MongoCredential.createScramSha1Credential("mdfmap_user", 
						"mdfmap_db", "Mongo123".toCharArray()));
				
		MongoDatabase database=null;

		MongoClient mongoClient2 = new MongoClient( seeds,mongocred);
		MongoCollection<Document> mongoTable2 = null;
		try{
			database = mongoClient2.getDatabase("mdfmap_db");
			mongoTable2  =   database.getCollection("mdf_pid_mappings");
			return mongoTable2;
		}catch(Exception e){}
		return null;
	}
	
	public static MongoCollection<Document> getmdfMetaDataSyncTable(){
		List<MongoCredential> mongocred = new ArrayList<MongoCredential>();
		List<ServerAddress> seeds = new ArrayList<ServerAddress>();

//		seeds.add( new ServerAddress( "mngdb-ebf-prd1-001.cisco.com" ,27048));
//		mongocred.add(MongoCredential.createScramSha1Credential("mdfmap_user", 
//				"mdfmap_db", "mongo123".toCharArray()));
		
		seeds.add( new ServerAddress( "mngdb-ebf-poe-02.cisco.com" ,27048));
		mongocred.add(MongoCredential.createScramSha1Credential("mdfmap_user", 
				"mdfmap_db", "Mongo123".toCharArray()));		
	
		MongoDatabase database=null;

		MongoClient mongoClient2 = new MongoClient( seeds,mongocred);
		MongoCollection<Document> mongoTable2 = null;
		try{
			database = mongoClient2.getDatabase("mdfmap_db");
			mongoTable2  =   database.getCollection("mdf_meta_datas");
			return mongoTable2;
		}catch(Exception e){}
		return null;
	}
	
	public static MongoCollection<Document> getSubLinesArchiveCollection(){
		List<MongoCredential> mongocred = new ArrayList<MongoCredential>();
		List<ServerAddress> seeds = new ArrayList<ServerAddress>();

		seeds.add( new ServerAddress( "mngdb-ebf-stg-01.cisco.com" ,27048));
		mongocred.add(MongoCredential.createScramSha1Credential("subscription_user", 
				"subscription_db", "webex123".toCharArray()));
	
		MongoDatabase database=null;

		MongoClient mongoClient2 = new MongoClient( seeds,mongocred);
		MongoCollection<Document> mongoTable2 = null;
		try{
			database = mongoClient2.getDatabase("subscription_db");
			mongoTable2  =   database.getCollection("subscription_lines_sync_archived");
			return mongoTable2;
		}catch(Exception e){}
		return null;
	}
	
	public static MongoCollection<Document> getEntitlementsArchiveCollection(){
		List<MongoCredential> mongocred2 = new ArrayList<MongoCredential>();
		List<ServerAddress> seeds2 = new ArrayList<ServerAddress>();

		seeds2.add( new ServerAddress( "mngdb-ebf-stg-01.cisco.com" ,27048));

		mongocred2.add(MongoCredential.createScramSha1Credential("entitlement_user", 
				"entitlement_db", "mongo123".toCharArray()));

		MongoDatabase database=null;

		MongoClient mongoClient2 = new MongoClient( seeds2,mongocred2);
		MongoCollection<Document> mongoTable2 = null;
		try{
			database = mongoClient2.getDatabase("entitlement_db");
			mongoTable2  =   database.getCollection("entitlements_archived");
			return mongoTable2;
		}catch(Exception e){}
		return null;
	}

}
