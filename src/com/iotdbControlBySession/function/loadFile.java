//�����ļ�
package com.iotdbControlBySession.function;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.qos.logback.core.filter.Filter;


public class loadFile {

	/**
	 * ���ж�ȡ�ļ��еļ�¼
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public ArrayList<String> fetchLine(String file) throws IOException{
		ArrayList<String> alStr = new ArrayList<String>();
		File f = new File(file);  
        BufferedReader reader = null;  
        try {  
            reader = new BufferedReader(new FileReader(f));  
            String tempString = null;  
            int line = 1;  
            while ((tempString = reader.readLine()) != null) {  
             	if(!tempString.startsWith("--")){//����--��ͷ������Ϊִ����䣬���м��ء�
                    alStr.add(tempString);
                    line++;  
            	}
            }  
            reader.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            if (reader != null) {  
                try {  
                    reader.close();  
                } catch (IOException e1) {  
                }  
            }  
        }  
        return alStr;
	}

	/**
	 * ���е����ݲ�ֳɣ�deviceID,�������ͣ����룬ѹ������ǩ�����ԣ�����������
	 * @param line
	 * @return
	 */
	public String[] fetchTSElement(String line){
		//��һ����::���
		String[] elementStr = line.split("::");
		return elementStr;
	}
	
	/**
	 * ��ֱ�ǩ,���� ���ļ��㽫[]�Ĳ��ְ��뿪��ÿ���ǩ����ֵ��Ϊһ�У������ַ�������
	 * @param line
	 * @return
	 */
	public String[] fetchTagArribute(String line){
		int charNum = line.length();
		String tempTxt = line.substring(1,charNum-1);
		System.out.println("tempTxt :"+tempTxt);
		String[] tagStr = tempTxt.split("==");
//		System.out.println("��ǩ���� :"+tagStr.length);
		return tagStr;
	}
	
	/**
	 * ��£��ǩ ���������� ��ÿ���ǩ����ֵ�ٲ�ֳ��µ��ַ������飬0��1�ֱ�put��map
	 * @param tagStr
	 * @return
	 */
	public String[] checkTagArribute(String tagStr){
		String[] tagGroup = tagStr.split("_");
		return tagGroup;
	}
	
	/**
	 * �������
	 * @param line
	 * @return
	 */
	public String[] fetchAttribute(String line){
		String[] attributeStr = line.split("_");
		return attributeStr;
	}

	//������飬һ�β���һ�м�¼����������Ϊ���������ͣ���������Ϊ����������
	public String[] insertRecordSLine(String line){
		String[] insertRecordSLine = line.split("::");
		return insertRecordSLine;
	}
	
}
