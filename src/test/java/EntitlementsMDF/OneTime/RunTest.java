package EntitlementsMDF.OneTime;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.BsonSerializationException;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.Test;

import com.cisco.DBConnections.GetConnection;
import com.cisco.utils.MongoDB;
import com.cisco.utils.OracleDB;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObjectCodec;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import junit.framework.Assert;

public class RunTest {
	
	public static void main(String[] args) {
		RunTest rt = new RunTest();
//		rt.Test2();  // Throwing exception in Oracle db for sytax what is col2=1
//		rt.Test5();
		rt.Test5_2();
	}

	/*@Test
	public void Test1() throws Throwable{

		String query ="select sum(count(*)) "
				+ "from rnp_admin.swc_rp_mdfid_pf_pids_map_gg group by "
				+ "Product_cat_mdfid, ERP_PRODUCT_NAME,Erp_product_family_name,Model_mdfid ";


		Hashtable<String,String[]> rows = getOracleData(query);

		int totalRecordsCountOracle=Integer.parseInt((rows.get("row_1"))[0]);
		long  totalRecordsCountMongo=0; 

		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		
		//get latest record from subscription_lines

		MongoCollection<Document>pidsmappingCollection = MongoDB.getmdfMappingsSyncTable();
		System.out.println("Find record in pids_mapping table");
		BasicDBObject whereQuery = new BasicDBObject();
		//FindIterable<Document> find =pidsmappingCollection.count();
		totalRecordsCountMongo = pidsmappingCollection.count();


		int counter=0;
		if(totalRecordsCountMongo != totalRecordsCountOracle){
			//check differnece of records
			int minlimit=1;
			int maxlimit =618025;
			counter=minlimit-1;
			query ="select Product_cat_mdfid, ERP_PRODUCT_NAME,Erp_product_family_name"
					+ ",Model_mdfid "
					+ "from rnp_admin.swc_rp_mdfid_pf_pids_map_gg "
					+ "group by product_cat_mdfid,erp_product_name,model_mdfid ,erp_product_family_name";

			Hashtable<String,String[]> limitedRowsOracle = getLimitedRecordsFromOracle(query,minlimit,maxlimit);
			for ( String key : limitedRowsOracle.keySet()){
				String [] row = limitedRowsOracle.get(key);
				BasicDBObject whereQuery2 = new BasicDBObject();
				whereQuery2.put("productCatMdfId", Long.parseLong(row[0]));
				whereQuery2.put("itemName", row[1]);
				whereQuery2.put("itemFamilyName", row[2]);
				whereQuery2.put("mdfLeafNodeId",Long.parseLong(row[3]));

				// productCatMdfid =268438303, itemName=S72BHK2-12101T, itemFamilyName=7200, mdfLeafNode=274392945
				long recordsFound= pidsmappingCollection.count(whereQuery2);
				System.out.print(++counter+",");
				if(counter ==minlimit){ 
					System.out.print("Starting record-"
							+ " productCatMdfid ="+ row[0]
									+ ", itemName="+ row[1]
											+ ", itemFamilyName="+ row[2]
													+ ", mdfLeafNode="+ row[3]+"\n");
				}

				if( recordsFound!=1){
					System.out.println("\nSearching record for-\n"
							+ " productCatMdfid ="+ row[0]
									+ ", itemName="+ row[1]
											+ ", itemFamilyName="+ row[2]
													+ ", mdfLeafNode="+ row[3]);
					System.out.println("--------instead of 1 record , found ="+recordsFound);

				}

				if(counter >=maxlimit){ 
					System.out.print("\nEnd record-"
							+ " productCatMdfid ="+ row[0]
									+ ", itemName="+ row[1]
											+ ", itemFamilyName="+ row[2]
													+ ", mdfLeafNode="+ row[3]);
				}
			}
		}

		Assert.assertTrue("Mongo records:"+totalRecordsCountMongo
				+" are not equal to"
				+ " Oracle records:"+totalRecordsCountOracle,totalRecordsCountMongo==totalRecordsCountOracle);

	}*/

	@Test
	public void Test2(){

		String [] oracleColumns={"model_mdfid","col2"};
		String [] mongoColumns={"mdfLeafNodeId","col2mongo"};
		String [] colValue={"1141","1"};
		String [] colValueType={"long","1"};

		String query ;
		Hashtable<String,String[]> rows;
		
		for(int i=0;i<oracleColumns.length;i++){
//			query="select product_cat_mdfid,erp_product_name,erp_product_family_name,model_mdfid"
//					+ " from rnp_admin.swc_rp_mdfid_pf_pids_map_gg"
//					+" where "+oracleColumns[i]+"="+colValue[i];

				
			System.out.println("Our   :"+oracleColumns[i]+"="+colValue[i]);
			query="select product_cat_mdfid,erp_product_name,erp_product_family_name,model_mdfid"
					+ " from rnp_admin.swc_rp_mdfid_pf_pids_map_gg"
					+" where model_mdfid=1141";
			
			rows = getOracleData(query);
			//int product_cat_mdfid=Integer.parseInt(rows.get("product_cat_mdfid").toString());		
			int totalRecordsCountOracle=rows.size();
			long  totalRecordsCountMongo=0; 

			MongoCursor<Document> cursor;
			//get latest record from subscription_lines
			MongoCollection<Document>pidsmappingCollection=null ;
			BasicDBObject whereQuery=null;
			try{
				pidsmappingCollection = MongoDB.getmdfMappingsSyncTable();
				System.out.println("Find record in pids_mapping table");
				whereQuery = new BasicDBObject();
				if(colValueType[i].equals("long")){
				
					whereQuery.put(mongoColumns[i], Long.parseLong(colValue[i]));
					System.out.println("whereQuery is :"+whereQuery);
				}else{
					whereQuery.put(mongoColumns[i], colValue[i]);
					System.out.println("whereQuery is :"+whereQuery);
				}
				
				//FindIterable<Document> find =pidsmappingCollection.count();
				totalRecordsCountMongo = pidsmappingCollection.count(whereQuery);

			}catch(Exception e){}

			
			
			List<String> oracleArrayList= new ArrayList<String>();
			List<String> mongoArrayList= new ArrayList<String>(); 
			if(totalRecordsCountOracle!=totalRecordsCountMongo ){
				//Hashtable<String,String[]> rows = getOracleData(query);
				//product_cat_mdfid,erp_product_name,erp_product_family_name,model_mdfid"
				//productCatMdfId,itemName,itemFamilyName,mdfLeafNodeId
				for ( String key : rows.keySet()){
					String [] row = rows.get(key);
					oracleArrayList.add(row[0]+"--"+row[1]+"--"+row[2]+"--"+row[3]);
				}
				
				FindIterable<Document> mongoRecords= pidsmappingCollection.find(whereQuery);
				for(Document d: mongoRecords){
					mongoArrayList.add(d.get("productCatMdfId").toString()+"--"+d.get("itemName").toString()
							+"--"+d.get("itemFamilyName").toString()+"--"+d.get("mdfLeafNodeId").toString());
				}
			}
			
			if(totalRecordsCountOracle>totalRecordsCountMongo){
				System.out.println("Missing records in Mongo:");
				System.out.println("product_cat_mdfid,erp_product_name,erp_product_family_name,model_mdfid");
				for(String oracleRecord: oracleArrayList){
					if(!mongoArrayList.contains(oracleRecord)){
						System.out.println(oracleRecord.replace("--", ","));
					}
				}
			}
			if(totalRecordsCountMongo>totalRecordsCountOracle){
				System.out.println("Missing records in Oracle:");
				System.out.println("productCatMdfId,itemName,itemFamilyName,mdfLeafNodeId");
				for(String mongoRecord: mongoArrayList){
					if(!oracleArrayList.contains(mongoRecord)){
						System.out.println(mongoRecord.replace("--", ","));
					}
				}
			}
//			Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);
		}




	}

	public static Hashtable<String,String[]> getOracleData(String query){
		OracleDB gdoDb=new OracleDB();
		int[] primaryKeys={0};
		Hashtable<String,String[]> totalRecords = new Hashtable<String,String[]>();

		Connection oracleConnection=null;
		try {
			oracleConnection = GetConnection.getOracleConnection();
			totalRecords = gdoDb.executeQuery(oracleConnection,query,primaryKeys);
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
		return totalRecords;
	}

	public static Hashtable<String,String[]> getLimitedRecordsFromOracle(String query,int minlimit,int maxlimit){
		OracleDB gdoDb=new OracleDB();
		int[] primaryKeys={0};
		Hashtable<String,String[]> totalRecords = new Hashtable<String,String[]>();

		Connection oracleConnection=null;
		try {
			oracleConnection = GetConnection.getOracleConnection();
			totalRecords = gdoDb.executeQueryLimitedReords(oracleConnection,query,primaryKeys,minlimit,maxlimit);
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
		return totalRecords;
	}

	public static long getOracleDataCount(String query){
		OracleDB gdoDb=new OracleDB();
		int[] primaryKeys={0};
		long recordsCount=-1;

		Connection oracleConnection=null;
		try {
			oracleConnection = GetConnection.getOracleConnection();
			recordsCount = gdoDb.executeQueryToGetCount(oracleConnection,query,primaryKeys);
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
		return recordsCount;
	}

	public static Hashtable<String,String[]> getOracleDataFromCG1(String query){
		OracleDB gdoDb=new OracleDB();
		int[] primaryKeys={0};
		Hashtable<String,String[]> totalRecords = new Hashtable<String,String[]>();

		Connection oracleConnection=null;
		try {
			oracleConnection = GetConnection.getOracleConnectionFromCG1();
			totalRecords = gdoDb.executeQuery(oracleConnection,query,primaryKeys);
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
		return totalRecords;
	}

	@Test
	public void Test3(){



		String query ="select distinct MDF_MODULES_CONCEPT_ID,mdf_hwseries_concept_id,"
				+ "mdf_modules_metaclass as metaclass,MDF_HWSERIES_LIFECYCLE  as  lifecycle "
				+ " from SWC_SDS_SERIES_HAS_MODULE" 
				+" where mdf_modules_metaclass='Interface/Module' and MDF_HWSERIES_LIFECYCLE='active'"
				+" union"
				+" select distinct mdf_concept_id, mdf_parent_concept_id ,metaclass as metaclass,lifecycle as lifecycle"
				+ " from shr_mdf_products_attr"  
				+" where metaclass='Interface/Module' and lifecycle='active'";

		String queryForCount ="select count(*) from ( "
				+query
				+" ) ";


		Hashtable<String,String[]> rowsForCount = getOracleData(queryForCount);

		int totalRecordsCountOracle=Integer.parseInt((rowsForCount.get("row_1"))[0]);
		long  totalRecordsCountMongo=0; 

		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		//get latest record from subscription_lines
		MongoCollection<Document> pidsmetadataCollection=null ;
		try{
			pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
//			System.out.println("Find record in metadata table");
			System.out.println("Find record in metadata table");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("metaclass","Interface/Module");
			whereQuery.put("lifecycle","active");
			//FindIterable<Document> find =pidsmappingCollection.count();
			totalRecordsCountMongo = pidsmetadataCollection.count(whereQuery);

		}catch(Exception e){}

		long counter=0;
		List<String> oracleArrayList= new ArrayList<String>();
		List<String> mongoArrayList= new ArrayList<String>(); 
		if(totalRecordsCountOracle!=totalRecordsCountMongo ){
			Hashtable<String,String[]> rows = getOracleData(query);
			//MDF_MODULES_CONCEPT_ID MDF_HWSERIES_CONCEPT_ID, 
			for ( String key : rows.keySet()){
				String [] row = rows.get(key);
				oracleArrayList.add(row[0]+"--"+row[1]);
			}
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("metaclass","Interface/Module");
			whereQuery.put("lifecycle","active");
			FindIterable<Document> mongoRecords= pidsmetadataCollection.find(whereQuery);
			for(Document d: mongoRecords){
				mongoArrayList.add(d.get("mdfLeafNodeId").toString()+"--"+d.get("mdfParentConceptId").toString());
			}
		}
		
		if(totalRecordsCountOracle>totalRecordsCountMongo){
			System.out.println("Missing records in Mongo:");
			System.out.println("MDF_MODULES_CONCEPT_ID,MDF_HWSERIES_CONCEPT_ID");
			for(String oracleRecord: oracleArrayList){
				if(!mongoArrayList.contains(oracleRecord)){
					System.out.println(oracleRecord.replace("--", ","));
				}
			}
		}
		if(totalRecordsCountMongo>totalRecordsCountOracle){
			System.out.println("Missing records in Oracle:");
			System.out.println("mdfLeafNodeId,mdfParentConceptId");
			for(String mongoRecord: mongoArrayList){
				if(!oracleArrayList.contains(mongoRecord)){
					System.out.println(mongoRecord.replace("--", ","));
				}
			}
		}
		//check differnece of records
//		Assert.assertTrue("Mongo records ("+totalRecordsCountMongo+") are"
//				+ " not equal to Oracle records("+totalRecordsCountOracle+")",totalRecordsCountMongo==totalRecordsCountOracle);

	}

	@Test
	public void Test4(){
		
 String query =" select distinct MDF_MODULES_CONCEPT_ID,mdf_hwseries_concept_id,mdf_modules_metaclass as metaclass from SWC_SDS_SERIES_HAS_MODULE where mdf_modules_metaclass='IOS Software Sub-SubCategory'"
+" union"
 +" select distinct mdf_concept_id, mdf_parent_concept_id ,metaclass as metaclass from shr_mdf_products_attr where metaclass='IOS Software Sub-SubCategory'";
 

	String queryForCount ="select count(*) from ( "
			+query
			+" ) ";


	Hashtable<String,String[]> rowsForCount = getOracleData(queryForCount);

	int totalRecordsCountOracle=Integer.parseInt((rowsForCount.get("row_1"))[0]);
	long  totalRecordsCountMongo=0; 
	
		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		//get latest record from subscription_lines
		MongoCollection<Document> pidsmetadataCollection =null;
		try{
			pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
			System.out.println("Find record in metadata table");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("metaclass","IOS Software Sub-SubCategory");
			//FindIterable<Document> find =pidsmappingCollection.count();
			totalRecordsCountMongo = pidsmetadataCollection.count(whereQuery);
		}catch(Exception e){}

		
		List<String> oracleArrayList= new ArrayList<String>();
		List<String> mongoArrayList= new ArrayList<String>(); 
		if(totalRecordsCountOracle!=totalRecordsCountMongo ){
			Hashtable<String,String[]> rows = getOracleData(query);
			//MDF_MODULES_CONCEPT_ID MDF_HWSERIES_CONCEPT_ID, 
			for ( String key : rows.keySet()){
				String [] row = rows.get(key);
				oracleArrayList.add(row[0]+"--"+row[1]);
			}
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("metaclass","IOS Software Sub-SubCategory");
			FindIterable<Document> mongoRecords= pidsmetadataCollection.find(whereQuery);
			for(Document d: mongoRecords){
				mongoArrayList.add(d.get("mdfLeafNodeId").toString()+"--"+d.get("mdfParentConceptId").toString());
			}
		}

		if(totalRecordsCountOracle>totalRecordsCountMongo){
			System.out.println("Missing records in Mongo:");
			System.out.println("MDF_MODULES_CONCEPT_ID,MDF_HWSERIES_CONCEPT_ID");
			for(String oracleRecord: oracleArrayList){
				if(!mongoArrayList.contains(oracleRecord)){
					System.out.println(oracleRecord.replace("--", ","));
				}
			}
		}
		if(totalRecordsCountMongo>totalRecordsCountOracle){
			System.out.println("Missing records in Oracle:");
			System.out.println("mdfLeafNodeId,mdfParentConceptId");
			for(String mongoRecord: mongoArrayList){
				if(!oracleArrayList.contains(mongoRecord)){
					System.out.println(mongoRecord.replace("--", ","));
				}
			}
		}
		
//		Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);

	}

	@Test
	public void Test5(){

		String query ="select distinct MDF_MODULES_CONCEPT_ID,mdf_hwseries_concept_id,mdf_hwseries_lifecycle as lifecycle from SWC_SDS_SERIES_HAS_MODULE where mdf_hwseries_lifecycle is null"
					+" union"
					+" select distinct mdf_concept_id, mdf_parent_concept_id ,lifecycle as lifecycle from shr_mdf_products_attr where lifecycle is null";

		String queryForCount ="select count(*) from ( "
				+query
				+" ) ";


		Hashtable<String,String[]> rowsForCount = getOracleData(queryForCount);

		int totalRecordsCountOracle=Integer.parseInt((rowsForCount.get("row_1"))[0]);
		long  totalRecordsCountMongo=0; 
		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		//get latest record from subscription_lines
		MongoCollection<Document> pidsmetadataCollection=null;
		try{
			pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
			System.out.println("Find record in metadata table");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("lifecycle","active");
			//FindIterable<Document> find =pidsmappingCollection.count();
			totalRecordsCountMongo = pidsmetadataCollection.count(whereQuery);

		}catch(Exception e){}

		List<String> oracleArrayList= new ArrayList<String>();
		List<String> mongoArrayList= new ArrayList<String>(); 
		if(totalRecordsCountOracle!=totalRecordsCountMongo ){
			Hashtable<String,String[]> rows = getOracleData(query);
			//MDF_MODULES_CONCEPT_ID MDF_HWSERIES_CONCEPT_ID, 
			for ( String key : rows.keySet()){
				String [] row = rows.get(key);
				oracleArrayList.add(row[0]+"--"+row[1]);
			}
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("lifecycle","active");
			FindIterable<Document> mongoRecords= pidsmetadataCollection.find(whereQuery);
			for(Document d: mongoRecords){
				mongoArrayList.add(d.get("mdfLeafNodeId").toString()+"--"+d.get("mdfParentConceptId").toString());
			}
		}

		if(totalRecordsCountOracle>totalRecordsCountMongo){
			System.out.println("Missing records in Mongo:");
			System.out.println("MDF_MODULES_CONCEPT_ID,MDF_HWSERIES_CONCEPT_ID");
			for(String oracleRecord: oracleArrayList){
				if(!mongoArrayList.contains(oracleRecord)){
					System.out.println(oracleRecord.replace("--", ","));
				}
			}
		}
		if(totalRecordsCountMongo>totalRecordsCountOracle){
			System.out.println("Missing records in Oracle:");
			System.out.println("mdfLeafNodeId,mdfParentConceptId");
			for(String mongoRecord: mongoArrayList){
				if(!oracleArrayList.contains(mongoRecord)){
					System.out.println(mongoRecord.replace("--", ","));
				}
			}
		}
		
//		Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);

	}

	/*@Test
	public void Test6(){

		String query ="select * from shr_mdf_products_attr"
				+" where (metaclass = 'Software Family' or metaclass = 'Model') and lifecycle='obsolete'";

		Hashtable<String,String[]> rows = getOracleData(query);
		int totalRecordsCountOracle=rows.size();
		long  totalRecordsCountMongo=0; 

		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		//get latest record from subscription_lines
		try{
			MongoCollection<Document>pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
			System.out.println("Find record in metadata table");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("lifecycle","obsolete");
			long  totalRows =pidsmetadataCollection.count(whereQuery);
			FindIterable<Document> docs= pidsmetadataCollection.find(whereQuery);
			Assert.assertTrue(totalRows==0);
			//FindIterable<Document> find =pidsmappingCollection.count();
			totalRecordsCountMongo = pidsmetadataCollection.count(whereQuery);

		}catch(Exception e){}
		System.out.println("totalRecordsCountMongo="+totalRecordsCountMongo);
		System.out.println("totalRecordsCountOracle="+totalRecordsCountOracle);
		Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);

	}*/

	/*@Test
	public void Test7(){
		String query ="select * from shr_mdf_products_attr"
				+" where (metaclass = 'Software Family' or metaclass = 'Model') and lifecycle!='obsolete'";

		Hashtable<String,String[]> rows = getOracleData(query);
		int totalRecordsCountOracle=rows.size();
		long  totalRecordsCountMongo=0; 

		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		//get latest record from subscription_lines
		MongoCollection<Document> pidsmetadataCollection=null;
		BasicDBObject whereQuery =null;
		try{
			pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
			System.out.println("Find record in metadata table");
			//BasicDBObject whereQuery = new BasicDBObject();
			BasicDBObject clause1 = new BasicDBObject("metaclass", "Software Family");  
			BasicDBObject clause2 = new BasicDBObject("metaclass", "Model");
			BasicDBObject clause3 = new BasicDBObject("lifecycle", new BasicDBObject("$ne","obsolete"));
			//todo: lifecycle !=obsolete
			BasicDBList or = new BasicDBList();

			BasicDBList andList1= new BasicDBList();
			andList1.add(clause1);
			andList1.add(clause3);

			BasicDBList andList2= new BasicDBList();
			andList2.add(clause2);
			andList2.add(clause3);

			BasicDBObject and1 =new BasicDBObject("$and",andList1);
			BasicDBObject and2 =new BasicDBObject("$and",andList2);

			or.add(and1);
			or.add(and2);

			whereQuery = new BasicDBObject("$or", or);
			//FindIterable<Document> find =pidsmappingCollection.count();
			totalRecordsCountMongo = pidsmetadataCollection.count(whereQuery);
			System.out.println(totalRecordsCountMongo);
		}catch(Exception e){}
		

		List<String> oracleArrayList= new ArrayList<String>();
		List<String> mongoArrayList= new ArrayList<String>(); 
		if(totalRecordsCountOracle!=totalRecordsCountMongo ){
			//Hashtable<String,String[]> rows = getOracleData(query);
			//MDF_MODULES_CONCEPT_ID MDF_HWSERIES_CONCEPT_ID, 
			for ( String key : rows.keySet()){
				String [] row = rows.get(key);
				oracleArrayList.add(row[0]+"--"+row[1]);
			}
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("lifecycle",null);
			FindIterable<Document> mongoRecords= pidsmetadataCollection.find(whereQuery);
			for(Document d: mongoRecords){
				mongoArrayList.add(d.get("mdfLeafNodeId").toString()+"--"+d.get("mdfParentConceptId").toString());
			}
		}

		if(totalRecordsCountOracle>totalRecordsCountMongo){
			System.out.println("Missing records in Mongo:");
			System.out.println("MDF_MODULES_CONCEPT_ID,MDF_HWSERIES_CONCEPT_ID");
			for(String oracleRecord: oracleArrayList){
				if(!mongoArrayList.contains(oracleRecord)){
					System.out.println(oracleRecord.replace("--", ","));
				}
			}
		}
		if(totalRecordsCountMongo>totalRecordsCountOracle){
			System.out.println("Missing records in Oracle:");
			System.out.println("mdfLeafNodeId,mdfParentConceptId");
			for(String mongoRecord: mongoArrayList){
				if(!oracleArrayList.contains(mongoRecord)){
					System.out.println(mongoRecord.replace("--", ","));
				}
			}
		}
		

		long counter=0;
		if(totalRecordsCountMongo != totalRecordsCountOracle){
			//check differnece of records
			counter=0;
			for ( String key : rows.keySet()){
				String [] row = rows.get(key);

				// product_cat_mdfid,erp_product_name,erp_product_family_name,model_mdfid
				System.out.print(++counter+",");
				BasicDBObject whereQuery2 = new BasicDBObject();
				//whereQuery.put(mongoColumns[i], colValue[i]);
				whereQuery2.put("productCatMdfid", Long.parseLong(row[0]));
				whereQuery2.put("itemName", row[1]);
				whereQuery2.put("itemFamilyName", row[2]);
				whereQuery2.put("mdfLeafNodeId",Long.parseLong(row[3]));

				long recordsFound= pidsmetadataCollection.count(whereQuery2);
				System.out.print(++counter+",");

				if(counter ==1){ 
					System.out.print("Starting record-"
							+ " productCatMdfid ="+ row[0]
									+ ", itemName="+ row[1]
											+ ", itemFamilyName="+ row[2]
													+ ", mdfLeafNode="+ row[3]+"\n");
				}

				if( recordsFound!=1){
					System.out.println("\nSearching record for-\n"
							+ " productCatMdfid ="+ row[0]
									+ ", itemName="+ row[1]
											+ ", itemFamilyName="+ row[2]
													+ ", mdfLeafNode="+ row[3]);
					System.out.println("--------instead of 1 record , found ="+recordsFound);

				}

			}
		}

		Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);

	}*/

	@Test
	public void Test8(){

		String query ="select mdf_concept_id from shr_mdf_products_attr"
				+ " where metaclass = 'Software Family' and lifecycle !='obsolete'";
		Hashtable<String,String[]> rows = getOracleData(query);
		int totalRecordsCountOracle=rows.size();

		//getting Data from mongo
		//get latest record from subscription_lines
		MongoCollection<Document> pidsmetadataCollection=null;
		for(int i=1;i<=totalRecordsCountOracle;i++){
			try{
				String mdfConceptId =rows.get("row_"+i)[0];;//rows.get("mdf_concept_id")[i];
				String query2 = "select * from shr_mdf_products_attr"
						+ " where metaclass = 'Software Version/Option' and lifecycle !='obsolete'"
						+ " and mdf_parent_concept_id="+mdfConceptId;
				Hashtable<String,String[]> rows2 = getOracleData(query2);
				long RecordsCount_oracle= rows2.size();

				pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
				System.out.println("Find record in metadata table");
				BasicDBObject whereQuery4 = new BasicDBObject();
				whereQuery4.put("mdfLeafNodeId",Long.parseLong(mdfConceptId));

				FindIterable<Document> doc = pidsmetadataCollection.find(whereQuery4);
				//db.getCollection('mdf_meta_datas').distinct('mdfNodeObjects',{mdfLeafNodeId:{$in:[279515766]}}).length
				//System.out.println(doc.toString());
				List<Document> mdfLeafNodeIdsArray= (List<Document>) doc.first().get("mdfNodeObjects");

				System.out.println("Mongo records:"+mdfLeafNodeIdsArray.size());
				System.out.println("Oracle records:"+RecordsCount_oracle);
				Assert.assertTrue("Mongo records:"+mdfLeafNodeIdsArray.size()
				+ " are not equal to "
				+ "Oracle records:"+RecordsCount_oracle
				,mdfLeafNodeIdsArray.size() == RecordsCount_oracle);
			}catch(Exception e){
				e.printStackTrace();
			}
			//todo:need to confirm
			long counter=0;
			//todo: fix temp variable
			long totalRecordsCountMongo =0;
			if(totalRecordsCountMongo != totalRecordsCountOracle){
				//check differnece of records
				counter=0;
				for ( String key : rows.keySet()){
					String [] row = rows.get(key);

					// product_cat_mdfid,erp_product_name,erp_product_family_name,model_mdfid
					System.out.print(++counter+",");
					BasicDBObject whereQuery2 = new BasicDBObject();
					//whereQuery.put(mongoColumns[i], colValue[i]);
					whereQuery2.put("productCatMdfid", Long.parseLong(row[0]));
					whereQuery2.put("itemName", row[1]);
					whereQuery2.put("itemFamilyName", row[2]);
					whereQuery2.put("mdfLeafNodeId",Long.parseLong(row[3]));

					long recordsFound= pidsmetadataCollection.count(whereQuery2);
					System.out.print(++counter+",");

					if(counter ==1){ 
						System.out.print("Starting record-"
								+ " productCatMdfid ="+ row[0]
										+ ", itemName="+ row[1]
												+ ", itemFamilyName="+ row[2]
														+ ", mdfLeafNode="+ row[3]+"\n");
					}

					if( recordsFound!=1){
						System.out.println("\nSearching record for-\n"
								+ " productCatMdfid ="+ row[0]
										+ ", itemName="+ row[1]
												+ ", itemFamilyName="+ row[2]
														+ ", mdfLeafNode="+ row[3]);
						System.out.println("--------instead of 1 record , found ="+recordsFound);

					}

				}
			}

		}
	}

	@Test
	public void Test10(){

		String query ="select mdf_parent_concept_id,mdf_concept_id from shr_mdf_products_attr"
				+ " where metaclass = 'Model' and lifecycle !='obsolete'";
		Hashtable<String,String[]> rows = getOracleData(query);
		int totalRecordsCountOracle=rows.size();

		//getting Data from mongo
		//get latest record from subscription_lines
		for( String key : rows.keySet()){
			String [] row = rows.get(key);
			try{
				String mdfParentConceptId =row[0];
				long mdfConceptId =Long.parseLong(row[1]);

				String query2 = "select mdf_hwseries_concept_id, mdf_modules_concept_id from swc_sds_series_has_module"
						+ " where mdf_modules_metaclass = 'Interface/Module' and MDF_HWSERIES_LIFECYCLE!='obsolete'"
						+ " and mdf_hwseries_concept_id="+mdfParentConceptId; //555
				
				Hashtable<String,String[]> rows2 = getOracleData(query2);
				long RecordsCount_oracle= rows2.size();

				MongoCollection<Document> pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
				System.out.println("Find record in metadata table");
				BasicDBObject whereQuery4 = new BasicDBObject();
				whereQuery4.put("mdfLeafNodeId",mdfConceptId);

				FindIterable<Document> doc = pidsmetadataCollection.find(whereQuery4);
				//db.getCollection('mdf_meta_datas').distinct('mdfNodeObjects',{mdfLeafNodeId:{$in:[279515766]}}).length
				List<Document> mdfLeafNodeIdsArray= (List<Document>) doc.first().get("mdfNodeObjects");

				//mdfLeafNodeIdsArray.size() == RecordsCount_oracle
				long counter=0;
				List<String> oracleArrayList= new ArrayList<String>();
				List<String> mongoArrayList= new ArrayList<String>(); 
				if(RecordsCount_oracle!=mdfLeafNodeIdsArray.size() ){
					 
					for ( String key2 : rows2.keySet()){
						String [] row2 = rows2.get(key2);
						oracleArrayList.add(row2[0]+"--"+row2[1]);
					}
					
					for(Document d: doc){
						mongoArrayList.add(d.get("mdfParentConceptId").toString()+"--"+d.get("mdfLeafNodeId").toString());
					}
				}
				//mdf_hwseries_concept_id, mdf_modules_concept_id
				//"mdfParentConceptId",mdfLeafNodeId
				
				if(RecordsCount_oracle>mdfLeafNodeIdsArray.size())
				{
					System.out.println("Missing records in Mongo:");
					System.out.println(query2);
					System.out.println("mdf_hwseries_concept_id, mdf_modules_concept_id");
					for(String oracleRecord: oracleArrayList){
						if(!mongoArrayList.contains(oracleRecord)){
							System.out.println(oracleRecord.replace("--", ","));
						}
					}
				}
				
			if(RecordsCount_oracle<mdfLeafNodeIdsArray.size())
			{
					System.out.println("Missing records in Oracle:");
					System.out.println("mdfParentConceptId,mdfLeafNodeId");
					for(String mongoRecord: mongoArrayList){
						if(!oracleArrayList.contains(mongoRecord)){
							System.out.println(mongoRecord.replace("--", ","));
						}
					}
				}
				
				
//				Assert.assertTrue("Mongo records are not equal to Oracle records"
//						,mdfLeafNodeIdsArray.size() == RecordsCount_oracle);
			}catch(Exception e){}


		}
	}

	@Test
	public void Test11(){

		String query ="select count * from shr_mdf_products_attr"
				+"where (metaclass = 'Software Family' and metaclass = 'Model')";

		Hashtable<String,String[]> rows = getOracleData(query);
		int totalRecordsCountOracle=rows.size();
		long  totalRecordsCountMongo=0; 

		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		//get latest record from subscription_lines
		MongoCollection<Document>pidsmetadataCollection =null;
		try{
			pidsmetadataCollection = MongoDB.getmdfMappingsSyncTable();
			System.out.println("Find record in metadata table");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("metaclass","Software Family");
			whereQuery.put("metaclass","Model");

			totalRecordsCountMongo = pidsmetadataCollection.count(whereQuery);

		}catch(Exception e){}
		long counter=0;
		if(totalRecordsCountMongo != totalRecordsCountOracle){
			//check differnece of records
			counter=0;
			for ( String key : rows.keySet()){
				String [] row = rows.get(key);

				// product_cat_mdfid,erp_product_name,erp_product_family_name,model_mdfid
				System.out.print(++counter+",");
				BasicDBObject whereQuery2 = new BasicDBObject();
				//whereQuery.put(mongoColumns[i], colValue[i]);
				whereQuery2.put("productCatMdfid", Long.parseLong(row[0]));
				whereQuery2.put("itemName", row[1]);
				whereQuery2.put("itemFamilyName", row[2]);
				whereQuery2.put("mdfLeafNodeId",Long.parseLong(row[3]));

				long recordsFound= pidsmetadataCollection.count(whereQuery2);
				System.out.print(++counter+",");

				if(counter ==1){ 
					System.out.print("Starting record-"
							+ " productCatMdfid ="+ row[0]
									+ ", itemName="+ row[1]
											+ ", itemFamilyName="+ row[2]
													+ ", mdfLeafNode="+ row[3]+"\n");
				}

				if( recordsFound!=1){
					System.out.println("\nSearching record for-\n"
							+ " productCatMdfid ="+ row[0]
									+ ", itemName="+ row[1]
											+ ", itemFamilyName="+ row[2]
													+ ", mdfLeafNode="+ row[3]);
					System.out.println("--------instead of 1 record , found ="+recordsFound);

				}

			}
		}
//		Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);

	}

	@Test
	public void Test12(){
		System.out.println("Running TC12");
		long inventory_item_id;
		String query ="select * from RNP_ADMIN.SWC_RP_MDFID_PF_PIDS_MAP_GG";
		Hashtable<String,String[]> rows = getOracleData(query);
		int totalRecordsCountOracle=rows.size();

		//getting Data from mongo
		//get latest record from subscription_lines
		for(int i=0;i<totalRecordsCountOracle;i++){
			try{
				String erpProductName =rows.get("erp_product_name")[i];
				String query2 = "SELECT msi. inventory_item_id,mic.category_id itemFamilyId"
						+"FROM mtl_system_items_b msi"
						+", MTL_ITEM_CATEGORIES mic"
						+", apps.MTL_CATEGORY_SETS mcs"
						+", apps.mtl_categories mc"
						+" WHERE 1 = 1"
						+"AND msi.inventory_item_id = mic.inventory_item_id"
						+"AND msi.organization_id = mic.organization_id"
						+"AND mic.category_set_id = 1100000245"
						+"AND mic.category_set_id = mcs.category_set_id"
						+"AND mcs.category_set_name = 'PROD GROUP'"
						+"AND mc.structure_id = mcs.structure_id"
						+"AND mic.category_id = mc.category_id"
						+"AND msi.organization_id = 1"
						+"AND msi.segment1 in '"+erpProductName+"'";
				Hashtable<String,String[]> rows2 = getOracleDataFromCG1(query2);
				long RecordsCount_oracle= rows2.size();
				inventory_item_id = Long.parseLong(rows2.get("inventory_item_id")[0]);

				////


				MongoCollection<Document> pidsmappingCollection = MongoDB.getmdfMappingsSyncTable();
				System.out.println("Find record in metadata table");
				BasicDBObject whereQuery4 = new BasicDBObject();
				whereQuery4.put("inventoryItemId",inventory_item_id);
				//////


				long MongoRowsCount = pidsmappingCollection.count(whereQuery4);
				Assert.assertTrue("Mongo records expected =1 , found="+MongoRowsCount
						,MongoRowsCount==1);
			}catch(Exception e){}


		}
	}

	/*@Test
	public void Test13(){

		String query ="select * from shr_mdf_products_attr"
				+"where metaclass = 'Software Family' and lifecycle ='obsolete'";

		Hashtable<String,String[]> rows = getOracleData(query);
		int totalRecordsCountOracle=rows.size();
		long  totalRecordsCountMongo=0; 

		//getting Data from mongo
		String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
		int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
		MongoCursor<Document> cursor;
		//get latest record from subscription_lines
		for(int i=0;i<totalRecordsCountOracle;i++){
			try{
				long mdfConceptId= Long.parseLong(rows.get("MDF_CONCEPT_ID")[i]);

				MongoCollection<Document>pidsmappingCollection = MongoDB.getmdfMappingsSyncTable();
				System.out.println("Find record in metadata table");
				BasicDBObject whereQuery = new BasicDBObject();
				whereQuery.put("mdfLeafNodeId",mdfConceptId);
				long  totalRows =pidsmappingCollection.count(whereQuery);
				System.out.println("totalRows = "+totalRows);
				Assert.assertTrue("total records should be =0", totalRows==0);

				MongoCollection<Document>pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
				System.out.println("Find record in metadata table");
				BasicDBObject whereQuery1 = new BasicDBObject();
				whereQuery.put("mdfLeafNodeId",mdfConceptId);
				long  totalRows1 =pidsmetadataCollection.count(whereQuery1);
				System.out.println("totalRows1 = "+totalRows);
				Assert.assertTrue("total records should be =0", totalRows1==0);

			}catch(Exception e){}


			Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);
		}
	}*/


@Test
public void TestNew() throws Throwable{

	String query ="select distinct model_mdfid "
			+ " from rnp_admin.swc_rp_mdfid_pf_pids_map_gg  " ;
			

	String queryForCount ="select count(*) from ( "
			+query
			+" ) ";


	Hashtable<String,String[]> rowsForCount = getOracleData(queryForCount);

	int totalRecordsCountOracle=Integer.parseInt((rowsForCount.get("row_1"))[0]);
	long  totalRecordsCountMongo=0; 

	List<String> oracleArrayList= new ArrayList<String>();
	List<String> mongoArrayList= new ArrayList<String>(); 
	
	MongoCollection<Document> pidsmetadataCollection=null ;
		pidsmetadataCollection = MongoDB.getmdfMappingsSyncTable();
		System.out.println("Find record in metadata table");
		
		//FindIterable<Document> find =pidsmappingCollection.count();
		DistinctIterable<BsonValue> mongoRecords = 
				pidsmetadataCollection.distinct("mdfLeafNodeId",BsonValue.class);
		totalRecordsCountMongo =0;
		MongoCursor<BsonValue> cursor=mongoRecords.iterator();
		while(cursor.hasNext()){
			String d =cursor.next().toString();
			
			mongoArrayList.add(d.toString());
			totalRecordsCountMongo++;
		}
	
	
	
	if(totalRecordsCountOracle!=totalRecordsCountMongo ){
		Hashtable<String,String[]> rows = getOracleData(query);
		//
		for ( String key : rows.keySet()){
			String [] row = rows.get(key);
			oracleArrayList.add(row[0]);
		}
		
	}
	
	if(totalRecordsCountOracle>totalRecordsCountMongo){
		System.out.println("Missing records in Mongo:");
		System.out.println("mdfLeafNodeId");
		for(String oracleRecord: oracleArrayList){
			if(!mongoArrayList.contains(oracleRecord)){
				System.out.println(oracleRecord);
			}
		}
	}
	if(totalRecordsCountMongo>totalRecordsCountOracle){
		System.out.println("Missing records in Oracle:");
		System.out.println("model_mdfid");
		for(String mongoRecord: mongoArrayList){
			if(!oracleArrayList.contains(mongoRecord)){
				System.out.println(mongoRecord);
			}
		}
	}
	//check differnece of records
//	Assert.assertTrue("Mongo records ("+totalRecordsCountMongo+") are"
//			+ " not equal to Oracle records("+totalRecordsCountOracle+")",totalRecordsCountMongo==totalRecordsCountOracle);

}

@Test
public void Test5_2(){

	String query ="select distinct MDF_MODULES_CONCEPT_ID,mdf_hwseries_concept_id,mdf_hwseries_lifecycle as lifecycle from SWC_SDS_SERIES_HAS_MODULE where mdf_hwseries_lifecycle = 'eol'"
				+" union"
				+" select distinct mdf_concept_id, mdf_parent_concept_id ,lifecycle as lifecycle from shr_mdf_products_attr where lifecycle = 'eol'";

	String queryForCount ="select count(*) from ( "
			+query
			+" ) ";
	
	System.out.println("queryForCount in oracle  :"+queryForCount);

	Hashtable<String,String[]> rowsForCount = getOracleData(queryForCount);

	int totalRecordsCountOracle=Integer.parseInt((rowsForCount.get("row_1"))[0]);
	
	System.out.println("totalRecordsCountOracle  is  :"+totalRecordsCountOracle);
	
	long  totalRecordsCountMongo=0; 
	//getting Data from mongo
	String itemFamilyName,createDate,itemName,secondaryMapped,itemType;
	int productCatMdfid,mdfLeafNodeId,inventoryItemId,itemFamilyId;
	MongoCursor<Document> cursor;
	
	BasicDBObject whereQuery = new BasicDBObject();
	whereQuery.put("lifecycle","eol");
	
	System.out.println("whereQuery i sin mongo  :"+whereQuery);
	
	//get latest record from subscription_lines
	MongoCollection<Document> pidsmetadataCollection=null;
	try{
		pidsmetadataCollection = MongoDB.getmdfMetaDataSyncTable();
		System.out.println("Find record in metadata collection");
//		BasicDBObject whereQuery = new BasicDBObject();
//		whereQuery.put("lifecycle",null);
		//FindIterable<Document> find =pidsmappingCollection.count();
		totalRecordsCountMongo = pidsmetadataCollection.count(whereQuery);
		System.out.println("totalRecordsCountMongo is :"+totalRecordsCountMongo);

	}catch(Exception e){}

	List<String> oracleArrayList= new ArrayList<String>();
	List<String> mongoArrayList= new ArrayList<String>(); 
	if(totalRecordsCountOracle!=totalRecordsCountMongo ){
		Hashtable<String,String[]> rows = getOracleData(query);
		//MDF_MODULES_CONCEPT_ID MDF_HWSERIES_CONCEPT_ID, 
		for ( String key : rows.keySet()){
			String [] row = rows.get(key);
			oracleArrayList.add(row[0]+"--"+row[1]);
		}
		
		System.out.println("oracleArrayList of records is   :"+oracleArrayList);
		
//		BasicDBObject whereQuery = new BasicDBObject();
//		whereQuery.put("lifecycle",null);
		FindIterable<Document> mongoRecords= pidsmetadataCollection.find(whereQuery);
		for(Document d: mongoRecords){
			mongoArrayList.add(d.get("mdfLeafNodeId").toString()+"--"+d.get("mdfParentConceptId").toString());
		}
		
		
		System.out.println("mongoArrayList of records is   :"+mongoArrayList);
	}

	if(totalRecordsCountOracle>totalRecordsCountMongo){
		System.out.println("Missing records in Mongo is :"+(totalRecordsCountOracle-totalRecordsCountMongo));
//		System.out.println("Count of missing records is  :"+(totalRecordsCountOracle-totalRecordsCountMongo));
		System.out.println("MDF_MODULES_CONCEPT_ID,MDF_HWSERIES_CONCEPT_ID");
		for(String oracleRecord: oracleArrayList){
			if(!mongoArrayList.contains(oracleRecord)){
				System.out.println(oracleRecord.replace("--", ","));
			}
		}
	}
	if(totalRecordsCountMongo>totalRecordsCountOracle){
		System.out.println("Missing records in Oracle is :"+(totalRecordsCountMongo-totalRecordsCountOracle));
//		System.out.println("Count of missing records is  :"+(totalRecordsCountMongo-totalRecordsCountOracle));
		System.out.println("mdfLeafNodeId,mdfParentConceptId");
		for(String mongoRecord: mongoArrayList){
			if(!oracleArrayList.contains(mongoRecord)){
				System.out.println(mongoRecord.replace("--", ","));
			}
		}
	}
	
//	Assert.assertTrue("Mongo records are not equal to Oracle records",totalRecordsCountMongo==totalRecordsCountOracle);

}
}