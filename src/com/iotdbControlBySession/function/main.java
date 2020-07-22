package com.iotdbControlBySession.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class main {

	public static void main(String[] args) {
		System.out.println("Control IOTDB by Session method . ");
		String dbip = args[0];
		String dbuser = "root";
		String dbpass = "root";
		
		String exeType = args[1];


		System.out.println("������");
		connectIotdb cit1 = new connectIotdb(dbip,dbuser,dbpass);
		connectIotdb cit = new connectIotdb();
		loadFile lf = new loadFile();
		
		if(exeType.equals("TEST")){
			System.out.println("���Է�֧");
			
		}
		
		if(exeType.equals("CSG")){
			System.out.println("�����洢�飺"+args[2]);
			cit.createGroup(args[2]);
		}
		if(exeType.equals("CTSS")){
			System.out.println("��������ʱ����Ϣ��");
			String sgName = "root."+args[2];
			String devName = args[3];
			String senName = args[4];
			TSDataType type = TSDataType.valueOf(args[5]);
			TSEncoding coding = TSEncoding.valueOf(args[6]);
			CompressionType compress = CompressionType.valueOf(args[7]);
			cit.createTimeseries(sgName, devName, senName, type, coding, compress);
		}
		if(exeType.equals("CTSM")){
			System.out.println("�������ʱ����Ϣ��");
			System.out.println("�����ļ�");
			List<String> lst = new ArrayList<String>();
			try {
				lst = lf.fetchLine(args[2]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			List<String> paths = new ArrayList<String>();
			List<TSDataType> dataTypes = new ArrayList<TSDataType>();
			List<TSEncoding> ecodings = new ArrayList<TSEncoding>();
			List<CompressionType> compressors = new ArrayList<CompressionType>();
			
			
			List<Map<String, String>> tagsList = new ArrayList<Map<String, String>>();
		     Map<String, String> tags = new HashMap<String, String>();
		     
		     List<Map<String, String>> attributesList = new ArrayList<Map<String, String>>();
		      Map<String, String> attributes = new HashMap<String, String>();
			
		      List<String> alias = new ArrayList<String>();
		      
			for(int i=0;i<lst.size();i++){
//				System.out.println(" -- : "+lst.get(i));
				String[] caseStr = lf.fetchTSElement(lst.get(i));
				paths.add(caseStr[0]);
				dataTypes.add(TSDataType.valueOf(caseStr[1]));
				ecodings.add(TSEncoding.valueOf(caseStr[2]));
				compressors.add(CompressionType.valueOf(caseStr[3]));
				int tagNum = lf.fetchTagArribute(caseStr[4]).length;
				for(int j=0;j<tagNum;j++){
					tags.put(lf.checkTagArribute(lf.fetchTagArribute(caseStr[4])[i])[0],lf.checkTagArribute(lf.fetchTagArribute(caseStr[4])[i])[0]);
				}
				tagsList.add(tags);
				int attNum = lf.fetchTagArribute(caseStr[5]).length;
				for(int j=0;j<attNum;j++){
					attributes.put(lf.checkTagArribute(lf.fetchTagArribute(caseStr[5])[i])[0],lf.checkTagArribute(lf.fetchTagArribute(caseStr[5])[i])[0]);
				}
				
				attributesList.add(attributes);
				alias.add(caseStr[6]);
			}
			cit.createMultiTimeseries(paths,dataTypes, ecodings, compressors, tagsList, attributesList, alias);
		}
		
		if(exeType.equals("IRS1")){
			System.out.println("����һ�� Record��һ�� Record ��һ���豸һ��ʱ����¶���������ݡ�");
			List<String> measurements = new ArrayList<String>();
			List<String> values = new ArrayList<String>();
			ArrayList<String> lineList = new ArrayList<String>();
			try {
				lineList = lf.fetchLine(args[2]);//IRS1
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for(int i=0;i<lineList.size();i++){//��ÿһ���������ֵ
				if(i==0){//�õ��������д�������
					String[] senName = lineList.get(i).split("==");
					for(int j=0;j<senName.length;j++){
						measurements.add(senName[j]);
					}
				}
				if(i==1){//�õ��������о���ֵ
					String[] val = lineList.get(i).split("==");
					for(int j=0;j<val.length;j++){
						values.add(val[j]);
					}
				}
			}
			cit.insert1("root."+args[3], measurements, values);
		}
		
		if(exeType.equals("IRS2")){
			System.out.println("����һ�� Record��һ�� Record ��һ���豸һ��ʱ����¶���������ݡ�");
			List<String> measurements = new ArrayList<String>();
			List<Object> values = new ArrayList<Object>();
			List<TSDataType> type = new ArrayList<TSDataType>();
			ArrayList<String> lineList = new ArrayList<String>();
			try {
				lineList = lf.fetchLine(args[2]);//IRS1
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for(int i=0;i<lineList.size();i++){//��ÿһ���������ֵ
				if(i==0){//�õ��������д�������
					String[] senName = lineList.get(i).split("==");
					for(int j=0;j<senName.length;j++){
						measurements.add(senName[j]);
					}
				}
				if(i==1){//�õ��������о���ֵ
					String[] val = lineList.get(i).split("==");
					for(int j=0;j<val.length;j++){
						values.add(val[j]);
					}
				}
				if(i==2){//�õ���ȫ����������
					String[] typeList = lineList.get(i).split("==");
					for(int k=0;k<typeList.length;k++){
						type.add(TSDataType.valueOf(typeList[k]));
					}
					
				}
			}
				cit.insert2("root."+args[3], measurements, type, values);
		}
		
		if(exeType.equals("IRM1")){
			System.out.println("������ Record����������Ҫ�������ƶ�");
			List<String> deviceIds = new ArrayList<String>();
			List<Long> times = new ArrayList<Long>();
			List<List<String>> measurementsList = new ArrayList<List<String>>();
			List<String> measure = new ArrayList<String>();
			List<List<String>> valuesList = new ArrayList<List<String>>();
			List<String> va = new ArrayList<String>();
			
			//��ͨ���ж�ȡ���õ�ÿһ�е����ݣ�Ȼ����||��λ�ü��ص�list
			ArrayList<String> fetchLine = new ArrayList<String>();
			try {
				fetchLine = lf.fetchLine(args[2]);//�����ļ�λ��
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//��deviceIds��list�м�������
			for(int i=0;i<fetchLine.size();i++){
				deviceIds.add(fetchLine.get(i).split("||")[0]);
			}
			//��times��list�����ʱ���
			for(int i=0;i<fetchLine.size();i++){
				times.add(Long.valueOf(fetchLine.get(i).split("||")[1]));
			}

			//�ڵ�����λ���Ȱ����еĴ�����������measuerelist
			for(int i=0;i<fetchLine.size();i++){
				String sensorLine = fetchLine.get(i).split("||")[2];
				String[] sensorList = sensorLine.split("==");
				for(int j=0;j<sensorList.length;j++){
					measure.add(sensorList[j]);
				}
				measurementsList.add(measure);
			}
			
			//�ѵ��ĸ�λ���ϵľ���ֵ������valuesList
			for(int i=0;i<fetchLine.size();i++){
				String valLine = fetchLine.get(i).split("||")[3];
				String[] valList = valLine.split("==");
				for(int j=0;j<valList.length;j++){
					va.add(valList[j]);
				}
				valuesList.add(va);
			}
			cit.insertRecords1(deviceIds, times, measurementsList, valuesList);
		}
		
		if(exeType.equals("IRM2")){

			List<String> deviceIds = new ArrayList<String>();
			List<Long> times = new ArrayList<Long>();
			List<List<String>> measurementsList = new ArrayList<List<String>>();
			List<String> measure = new ArrayList<String>();
			List<List<Object>> valuesList = new ArrayList<List<Object>>();
			List<Object> va = new ArrayList<Object>();
			List<List<TSDataType>> types = new ArrayList<List<TSDataType>>();
			List<TSDataType> tp = new ArrayList<TSDataType>();
			
			//��ͨ���ж�ȡ���õ�ÿһ�е����ݣ�Ȼ����||��λ�ü��ص�list
			ArrayList<String> fetchLine = new ArrayList<String>();
			try {
				fetchLine = lf.fetchLine(args[2]);//�����ļ�λ��
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//��deviceIds��list�м�������
			for(int i=0;i<fetchLine.size();i++){
				deviceIds.add(fetchLine.get(i).split("||")[0]);
			}
			//��times��list�����ʱ���
			for(int i=0;i<fetchLine.size();i++){
				times.add(Long.valueOf(fetchLine.get(i).split("||")[1]));
			}

			//�ڵ�����λ���Ȱ����еĴ�����������measuerelist
			for(int i=0;i<fetchLine.size();i++){
				String sensorLine = fetchLine.get(i).split("||")[2];
				String[] sensorList = sensorLine.split("==");
				for(int j=0;j<sensorList.length;j++){
					measure.add(sensorList[j]);
				}
				measurementsList.add(measure);
			}
			
			//�ѵ��ĸ�λ���ϵľ���ֵ������valuesList
			for(int i=0;i<fetchLine.size();i++){
				String valLine = fetchLine.get(i).split("||")[3];
				String[] valList = valLine.split("==");
				for(int j=0;j<valList.length;j++){
					va.add(valList[j]);
				}
				valuesList.add(va);
			}
			//�ѵ����λ���ϵ��������ͣ�����types
			for(int i=0;i<fetchLine.size();i++){
				String typeLine = fetchLine.get(i).split("||")[4];
				String[] tpyeList = typeLine.split("==");
				for(int j=0;j<tpyeList.length;j++){
					tp.add(TSDataType.valueOf(tpyeList[j]));
				}
				types.add(tp);
			}
			
			cit.insertRecords2(deviceIds, times, measurementsList,types, valuesList);
		}
		
		if(exeType.equals("ITB1")){
			System.out.println("����һ�� Tablet");
//			int maxRow = Integer.valueOf(args[2]);
//			String gpPath = "root."+args[3];
			
			
			
//			List<MeasurementSchema> schemaList = new ArrayList<MeasurementSchema>();
//			schemaList.add(new MeasurementSchema("deva", TSDataType.INT64, TSEncoding.PLAIN));
//			schemaList.add(new MeasurementSchema("devb", TSDataType.INT32, TSEncoding.PLAIN));
////			schemaList.add(new MeasurementSchema("devc", TSDataType.TEXT, TSEncoding.PLAIN));
			
//			cit.itb1("sg_ln5",3,"Ba2","INT32","PLAIN");
			try {
				cit.itb2();
			} catch (IoTDBConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StatementExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			    
//			List<String> dataTypes = new ArrayList<String>();
//			dataTypes.add("TEXT");
//			dataTypes.add("TEXT");
//			long time = 0;
//			List<String> value = new ArrayList<String>();
//			value.add("'abc'");
//			value.add("'abc'");
//			System.out.println("������ݼ���");
//			cit.insertTablet(schemaList, gpPath, maxRow,dataTypes,time);
		}
		
		if(exeType.equals("ITB2")){
			System.out.println("һ�β�����tablet,������");
		}
		
		if(exeType.equals("DTS1")){
			System.out.println("ɾ��һ��ʱ������");
			cit.deleteTimeseries(args[2]);
		}
		
		if(exeType.equals("DTS2")){
			System.out.println("һ��ɾ�����ʱ������");
			List<String> paths = new ArrayList<String>();
			String line = "";
			//���ļ����ж�ȡһ��Ҫɾ����path
			try {
				for(int i=0;i<lf.fetchLine(args[2]).size();i++){
					line = lf.fetchLine(args[2]).get(i);
					String[] pathList = line.split("||");
					for(int j=0;j<pathList.length;j++){
						paths.add(pathList[j]);
					}
					try {
						cit.deleteTimeseries(paths);
					} catch (IoTDBConnectionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (StatementExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
		
		if(exeType.equals("DTR1")){
			System.out.println("ɾ��һ��ʱ��������ĳ��ʱ���֮ǰ�ļ�¼��");
			cit.deleteData(args[2], Long.valueOf(args[3]));//����1 Ҫɾ����ʱ��������Ϣ������2 Ҫɾ����ʱ���
			
		}
		
		if(exeType.equals("DTR2")){
			System.out.println("ɾ�����ʱ��������ĳ��ʱ���֮ǰ�ļ�¼��");
			List<String> paths = new ArrayList<String>();
			String line = "";
			//���ļ�����Ҫɾ����path�б�
			try {
				for(int i=0;i<lf.fetchLine(args[2]).size();i++){
					line = lf.fetchLine(args[2]).get(i);
					String[] pathList = line.split("||");
					for(int j=0;j<pathList.length;j++){
						paths.add(pathList[j]);
					}
					cit.deleteData(paths, Long.valueOf(args[3]));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		

		System.out.println("�ر�����");
		connectIotdb cit2 = new connectIotdb("close");

		
	}

}
