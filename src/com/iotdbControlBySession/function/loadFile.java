//加载文件
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
	 * 按行读取文件中的记录
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
             	if(!tempString.startsWith("--")){//不以--开头，被视为执行语句，进行加载。
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
	 * 将行的内容拆分成：deviceID,数据类型，编码，压缩，标签，属性，别名等内容
	 * @param line
	 * @return
	 */
	public String[] fetchTSElement(String line){
		//第一层用::拆分
		String[] elementStr = line.split("::");
		return elementStr;
	}
	
	/**
	 * 拆分标签,属性 从文件层将[]的部分剥离开，每组标签名和值作为一行，返回字符串数组
	 * @param line
	 * @return
	 */
	public String[] fetchTagArribute(String line){
		int charNum = line.length();
		String tempTxt = line.substring(1,charNum-1);
		System.out.println("tempTxt :"+tempTxt);
		String[] tagStr = tempTxt.split("==");
//		System.out.println("标签数量 :"+tagStr.length);
		return tagStr;
	}
	
	/**
	 * 归拢标签 ，属性内容 将每组标签名和值再拆分成新的字符串数组，0，1分别put进map
	 * @param tagStr
	 * @return
	 */
	public String[] checkTagArribute(String tagStr){
		String[] tagGroup = tagStr.split("_");
		return tagGroup;
	}
	
	/**
	 * 拆分属性
	 * @param line
	 * @return
	 */
	public String[] fetchAttribute(String line){
		String[] attributeStr = line.split("_");
		return attributeStr;
	}

	//拆分数组，一次插入一行记录，两段数据为无数据类型，三段数据为有数据类型
	public String[] insertRecordSLine(String line){
		String[] insertRecordSLine = line.split("::");
		return insertRecordSLine;
	}
	
}
