package com.iotdbControlBySession.function;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
//import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;


public class connectIotdb {

	private static Session session;
	private String dbip;
	private String dbuser;
	private String dbpass;

	//构造函数
	public connectIotdb(String dbip,String dbuser,String dbpass){
		//打开链接
		session = new Session(dbip, 6667, dbuser, dbpass);
		try {
			session.open();
		} catch (IoTDBConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public connectIotdb(){
		//打开链接
//		System.out.println("无参数构造函数：");
	}
	
	
	public connectIotdb(String controlDB){
		if(controlDB.equals("close")){
			try {
				session.close();
			} catch (IoTDBConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public int randomNum(){
		int deviceNum = (int)(Math.random()*10+1);
		return deviceNum;
	}
	
	/**
	 * 创建存储组
	 * @param gpName 存储组名
	 */
	public void createGroup(String gpName){
//		for(int i=gpStart;i<gpNum+gpStart;i++){
			try {
				session.setStorageGroup("root."+gpName);
			} catch (IoTDBConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StatementExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//		}
	}
	
	/**
	 * 创建设备和时序信息
	 * @param gpName 存储组名
	 * @param gpNum 存储组数量
	 * @param devName 设备名
	 * @param devNum 设备数量每个组的设备数量
	 * @param tsName 时序名
	 * @param tsNum 时序数每个设备的时序数量
	 * @param datatype 数据类型
	 * @param ecode 数据编码
	 */
	public void createTimeseries(String gpName,String devName,String tsName,TSDataType dataType,TSEncoding  ecoding,CompressionType  isCompress){

					try {
						session.createTimeseries("root."+gpName+"."+devName+"."+tsName, dataType, ecoding, isCompress);
					} catch (IoTDBConnectionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (StatementExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

	}
	
	
	/**
	 * 创建多个时间序列
	 * @param paths
	 * @param dataTypes
	 * @param encodings
	 * @param compressors
	 * @param propsList
	 * @param tagsList
	 * @param attributesList
	 * @param measurementAliasList
	 */
	void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
	        List<TSEncoding> encodings, List<CompressionType> compressors,
	        List<Map<String, String>> tagsList,
	        List<Map<String, String>> attributesList, List<String> measurementAliasList){
		 try {
			session
			 .createMultiTimeseries(paths, dataTypes, encodings, compressors, null, tagsList,
			     attributesList, measurementAliasList);
		} catch (IoTDBConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StatementExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	/**
	 * 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。服务器需要做类型推断，可能会有额外耗时
	 * @param deviceId
	 * @param time
	 * @param measurements
	 * @param values
	 */
	public void insert1(String deviceId,List<String> measurements, List<String> values) {
		Date dt= new Date();
		long timeseries = dt.getTime();
			try {
				session.insertRecord(deviceId, timeseries, measurements, values);
			} catch (IoTDBConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StatementExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    }

	/**
	 * 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。提供数据类型后，服务器不需要做类型推断，可以提高性能
		 * @param deviceId
		 * @param measurements
		 * @param type
		 * @param values
		 */
	public void insert2(String deviceId,List<String> measurements,List<TSDataType> type, List<Object> values){
		Date dt= new Date();
		long timeseries = dt.getTime();
			try {
			   session.insertRecord(deviceId, timeseries, measurements, type, values);
			} catch (IoTDBConnectionException e) {
				e.printStackTrace();
			} catch (StatementExecutionException e) {
				e.printStackTrace();
			}
	}

	 /**
	   * 插入多个 Record。服务器需要做类型推断，可能会有额外耗时
	   */
	 public void insertRecords1(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList){
		   
		        try {
					session.insertRecords(deviceIds, times, measurementsList,  valuesList);
					
				} catch (IoTDBConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (StatementExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	  
	  /**
	   * 插入多个 Record。提供数据类型后，服务器不需要做类型推断，可以提高性能
	   * @param deviceIds
	   * @param times
	   * @param measurementsList
	   * @param typesList
	   * @param valuesList
	   */
	 public void insertRecords2(List<String> deviceIds, List<Long> times,List<List<String>> measurementsList, List<List<TSDataType>> typesList,
		      List<List<Object>> valuesList){
		 try {
				session.insertRecords(deviceIds, times, measurementsList,typesList,  valuesList);
				
			} catch (IoTDBConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StatementExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 }
	
	 
	 public void itb2() throws IoTDBConnectionException, StatementExecutionException{
		  // The schema of sensors of one device
		    List<MeasurementSchema> schemaList = new ArrayList<>();
		    schemaList.add(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.PLAIN));
		    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.PLAIN));
		    schemaList.add(new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.PLAIN));

		    Tablet tablet = new Tablet("root.sg1.d3", schemaList, 100);

		    long[] timestamps = tablet.timestamps;
		    Object[] values = tablet.values;

		    for (long time = 0; time < 100; time++) {
		      int row = tablet.rowSize++;
		      timestamps[row] = time;
		      for (int i = 0; i < 3; i++) {
		        int[] sensor = (int[]) values[i];
		        sensor[row] = i;
		      }
		      if (tablet.rowSize == tablet.getMaxRowNumber()) {
		        session.insertTablet(tablet, true);
		        tablet.reset();
		      }
		    }

		    if (tablet.rowSize != 0) {
		      session.insertTablet(tablet);
		      tablet.reset();
		    }

	 }
	 
	 
	 public void itb1(String sgName,int senNum,String senName,String dataType,String ecoding){
		 List<MeasurementSchema> schemaList = new ArrayList<MeasurementSchema>();
		if(dataType.equals("INT64")){
			if(ecoding.equals("PLAIN")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT64, TSEncoding.PLAIN));
			 	}
			}
			if(ecoding.equals("RLE")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT64, TSEncoding.RLE));
			 	}
			}
			if(ecoding.equals("TS_2DIFF")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT64, TSEncoding.TS_2DIFF));
			 	}
			}
			if(ecoding.equals("REGULAR")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT64, TSEncoding.REGULAR));
			 	}
			}
		}
		if(dataType.equals("INT32")){
			if(ecoding.equals("PLAIN")){
				System.out.println(" -- int32 -- plain");
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT32, TSEncoding.PLAIN));
			 	}
			}
			if(ecoding.equals("RLE")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT32, TSEncoding.RLE));
			 	}
			}
			if(ecoding.equals("TS_2DIFF")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT32, TSEncoding.TS_2DIFF));
			 	}
			}
			if(ecoding.equals("REGULAR")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.INT32, TSEncoding.REGULAR));
			 	}
			}
		}
		if(dataType.equals("BOOLEAN")){
			if(ecoding.equals("PLAIN")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.BOOLEAN, TSEncoding.PLAIN));
			 	}
			}
			if(ecoding.equals("RLE")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.BOOLEAN, TSEncoding.RLE));
			 	}
			}
		}
		if(dataType.equals("DOUBLE")){
			if(ecoding.equals("PLAIN")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.DOUBLE, TSEncoding.PLAIN));
			 	}
			}
			if(ecoding.equals("RLE")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.DOUBLE, TSEncoding.RLE));
			 	}
			}
			if(ecoding.equals("TS_2DIFF")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.DOUBLE, TSEncoding.TS_2DIFF));
			 	}
			}
			if(ecoding.equals("GORILLA")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.DOUBLE, TSEncoding.GORILLA));
			 	}
			}
		}
		if(dataType.equals("FLOAT")){
			if(ecoding.equals("PLAIN")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.FLOAT, TSEncoding.PLAIN));
			 	}
			}
			if(ecoding.equals("RLE")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.FLOAT, TSEncoding.RLE));
			 	}
			}
			if(ecoding.equals("TS_2DIFF")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.FLOAT, TSEncoding.TS_2DIFF));
			 	}
			}
			if(ecoding.equals("GORILLA")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.FLOAT, TSEncoding.GORILLA));
			 	}
			}
		}
		if(dataType.equals("TEXT")){
			if(ecoding.equals("PLAIN")){
				for(int i=0;i<=senNum;i++){
			 		schemaList.add(new MeasurementSchema(senName+"_"+i, TSDataType.TEXT, TSEncoding.PLAIN));
			 	}
			}
		}
		  
		 Tablet tablet = new Tablet("root."+sgName, schemaList, 5);

		    long[] timestamps = tablet.timestamps;
		    Object[] values = tablet.values;

		    
		    for (long time = 0; time < 10; time++) {
		      int row = tablet.rowSize++;
		      timestamps[row] = time;
		      for (int i = 0; i <= senNum; i++) {
		    	  if(dataType.equals("FLOAT")){
				        float[] sensor = (float[]) values[i];
				        sensor[row] = i;
		    	  }
		    	  if(dataType.equals("INT64")){
				        long[] sensor = (long[]) values[i];
				        sensor[row] = i;
		    	  }
		    	  if(dataType.equals("INT32")){
				        int[] sensor = (int[]) values[i];
				        sensor[row] = i;
		    	  }
		    	  if(dataType.equals("DOUBLE")){
				        int[] sensor = (int[]) values[i];
				        sensor[row] = i;
		    	  }
		    	  if(dataType.equals("BOOLEAN")){
				        int[] sensor = (int[]) values[i];
				        sensor[row] = 1;
		    	  }
		    	  if(dataType.equals("TEXT")){
				        int[] sensor = (int[]) values[i];
				        sensor[row] = i;
		    	  }
		    	  
		      }
		      if (tablet.rowSize == tablet.getMaxRowNumber()) {
		        try {
					session.insertTablet(tablet, true);
				} catch (IoTDBConnectionException e) {
					e.printStackTrace();
				} catch (StatementExecutionException e) {
					e.printStackTrace();
				}
		        tablet.reset();
		      }
		    }

		    if (tablet.rowSize != 0) {
		      try {
				session.insertTablet(tablet);
			} catch (StatementExecutionException e) {
				e.printStackTrace();
			} catch (IoTDBConnectionException e) {
				e.printStackTrace();
			}
		      tablet.reset();
		    }

	 }
	 
	 
	 /**
	   * 插入一个 Tablet,Tablet 是一个设备若干行非空数据块，每一行的列都相同
	   */
	  public void insertTablet(List<MeasurementSchema> schemaList,String gpPath,int maxRow,List<String> dataTypes,long time){

		    Tablet tablet = new Tablet(gpPath, schemaList, maxRow);
		    long[] timestamps = tablet.timestamps;
		    Object[] values = tablet.values;
		    for (long timer = time; timer < timer + maxRow; timer++) {
		      int row = tablet.rowSize++;
		      timestamps[row] = timer;
		      for (int i = 0; i < schemaList.size(); i++) {
		    	  System.out.println(" ----- 1a --------"+i);
		    	  long[] sensor = (long[]) values[i];
		    	  System.out.println(" ----- 2b --------"+i);
		          sensor[row] = i;
		          System.out.println(" ----- 3c --------"+i);

//		    	if(dataTypes.get(i).equals("INT64")){
//		    		Object[] sensor = (Object[])values[i];
//		    		sensor[row] = value.get(i);
//		    	}
//		    	if(dataTypes.get(i).equals("INT32")){
//		    		Object[] sensor = (Object[])values[i];
//		    		sensor[row] = value.get(i);
//		    	}
//		    	if(dataTypes.get(i).equals("FLOAT")){
//		    		Object[] sensor = (Object[])values[i];
//		    		sensor[row] = value.get(i);
//		    	}
//		    	if(dataTypes.get(i).equals("DOUBLE")){
//		    		Object[] sensor = (Object[])values[i];
//		    		sensor[row] = value.get(i);
//		    	}
//		    	if(dataTypes.get(i).equals("BOOLEAN")){
//		    		Object[] sensor = (Object[])values[i];
//		    		sensor[row] = value.get(i);
//		    	}
//		    	if(dataTypes.get(i).equals("TEXT")){
//		    		System.out.println(" --- 6 --- ");
//		    		String[] sensor = (String[])values[i];
//
//		    		System.out.println(" --- 7 --- ");
//		    		sensor[row] = ""+i+"";
//		    		System.out.println(" --- 8 --- ");
//		    	}
		        
		      }
		      if (tablet.rowSize == tablet.getMaxRowNumber()) {
		        try {
					session.insertTablet(tablet, true);
				} catch (IoTDBConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (StatementExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        tablet.reset();
		      }
		    }
		    if (tablet.rowSize != 0) {
		        try {
					session.insertTablet(tablet);
				} catch (StatementExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IoTDBConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        tablet.reset();
		      }

	  }
	 
	  /**
	   * 插入多个Tablet
	   * @param tablet
	   * @throws IoTDBConnectionException
	   * @throws StatementExecutionException
	   */
	  public void insertTablets(Map<String, Tablet> tablet,List<MeasurementSchema> schemaList,int idx,String gpPath)  throws IoTDBConnectionException, StatementExecutionException {
//		  List<MeasurementSchema> schemaList = new ArrayList<MeasurementSchema>();
		    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
		    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
		    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));
//
		    Tablet tablet1 = new Tablet("root.sg1.d1", schemaList, 100);
		    Tablet tablet2 = new Tablet("root.sg1.d2", schemaList, 100);
		    Tablet tablet3 = new Tablet("root.sg1.d3", schemaList, 100);

		    Map<String, Tablet> tabletMap = new HashMap<String, Tablet>();
		    tabletMap.put("root.sg1.d1", tablet1);
		    tabletMap.put("root.sg1.d2", tablet2);
		    tabletMap.put("root.sg1.d3", tablet3);
		  
		  	

		  	for(int i=0;i<tablet.size();i++){
		  		Tablet table_i = new Tablet(gpPath,schemaList,20);
		  		
		  	}
//		  	for(int i=0;i<tablet.size();i++){
//		  		long[] timestamps_i = table_i.
//		  	}
		  
		    long[] timestamps1 = tablet1.timestamps;
		    Object[] values1 = tablet1.values;
		    long[] timestamps2 = tablet2.timestamps;
		    Object[] values2 = tablet2.values;
		    long[] timestamps3 = tablet3.timestamps;
		    Object[] values3 = tablet3.values;

		    for (long time = 0; time < 100; time++) {
		      int row1 = tablet1.rowSize++;
		      int row2 = tablet2.rowSize++;
		      int row3 = tablet3.rowSize++;
		      timestamps1[row1] = time;
		      timestamps2[row2] = time;
		      timestamps3[row3] = time;
		      for (int i = 0; i < 3; i++) {
		        long[] sensor1 = (long[]) values1[i];
		        sensor1[row1] = i;
		        long[] sensor2 = (long[]) values2[i];
		        sensor2[row2] = i;
		        long[] sensor3 = (long[]) values3[i];
		        sensor3[row3] = i;
		      }
		      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
		        session.insertTablets(tabletMap, true);

		        tablet1.reset();
		        tablet2.reset();
		        tablet3.reset();
		      }
		    }

		    if (tablet1.rowSize != 0) {
		      session.insertTablets(tabletMap, true);
		      tablet1.reset();
		      tablet2.reset();
		      tablet3.reset();
		    }

	  }
	  
	/**
	 * 删除一个时间序列
	 * @param path
	 */
	public void deleteTimeseries(String path){
		 try {
			session.deleteTimeseries(path);
		} catch (IoTDBConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StatementExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	/**
	 * 删除多个时间序列
	 */
	  public void deleteTimeseries(List<String> paths)
		      throws IoTDBConnectionException, StatementExecutionException {
		    session.deleteTimeseries(paths);
		  }

	  /**
	   * 删除一个时间序列在某个时间点前的数据
	   * @param path
	   * @param time
	   */
	  public void deleteData(String path, long time){
		 
		    try {
				session.deleteData(path, time);
			} catch (IoTDBConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StatementExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	  }
	  
	  
	  /**
	   * 删除多个时间序列在某个时间点前的数据
	   * @param paths
	   * @param time
	   */
	  public void deleteData(List<String> paths, long time){
		  try {
			session.deleteData(paths, time);
		} catch (IoTDBConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StatementExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	  

	
	  
	  
	 
	  
	 
	  

	  
	/**
	 * 循环批量session插入记录
	 */
	public void batchInsert(){
		Schema schema = new Schema();
		Date dt= new Date();
		long timeseries = dt.getTime();
		
		for(int j=0;j<10;j++){
			for(int i=0;i<10;i++){
				
				String deviceId = "root.ln"+i+".d"+j+"";
				List<String> measurements = new ArrayList<String>();
				    measurements.add("ch1");
				    measurements.add("ch2");
				    measurements.add("ch3");
				    measurements.add("ch4");
				    measurements.add("ch5");
				    measurements.add("ch7");
				    measurements.add("ch8");
				    measurements.add("ch9");
				    measurements.add("ch11");
				    measurements.add("ch12");
				    measurements.add("ch13");
				    measurements.add("ch14");
				    measurements.add("ch15");
				    measurements.add("ch16");
				    measurements.add("ch17");
				    measurements.add("ch18");
				    measurements.add("ch19");
				    for (long time = timeseries; time < timeseries+100; time++) {
					      List<String> values = new ArrayList<String>();
					      values.add("false");
					      values.add("false");
					      values.add("3");
					      values.add("1");
					      values.add("2");
					      values.add("3");
					      values.add("1");
					      values.add("2");
					      values.add("3");
					      values.add("1");
					      values.add("2");
					      values.add("3");
					      values.add("1");
					      values.add("2");
					      values.add("3");
					      values.add("1");
					      values.add("2");
					      values.add("3");
					      values.add("1");
					      values.add("2");
					      values.add("'3'");
					      try {
							session.insertRecord(deviceId, time, measurements, values);
						} catch (IoTDBConnectionException eBatchInsert) {
							// TODO Auto-generated catch block
							eBatchInsert.printStackTrace();
						} catch (StatementExecutionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    }
			}
		}
	}
	
}
