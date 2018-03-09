package com.cognizant.ddhkafka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;
import org.joda.time.Minutes;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileWatcher {
	
	private File dir = new File("/users/ngvinay/bigdata/kafka/data/");
	SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
	
	
	public void checkFiles(){
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		String currentTs = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
		String jsonFile;
		Date lmts = null;
		Date cts = null;
		try {
			cts = format.parse(currentTs);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	
		// To list only the files that end with .txt
		FilenameFilter textFilter = new FilenameFilter() {
		public boolean accept(File dir, String name) {
			String lowercaseName = name.toLowerCase();
			if (lowercaseName.endsWith(".txt")) {
				return true;
			} else {
				return false;
			}
		}
		};
		
		// Lists all the .txt files present in the data folder
		File[] files = dir.listFiles(textFilter);
		
		if (files.length == 0){
			System.out.println("No new files for filewatcher");
		}
		
		// Loop thru each .txt file and rename to .json if the file is created a minute before the current timestamp
		for (File file : files) {
			
			String[] filename = file.getName().split("\\.");			
			jsonFile = filename[0] + ".json";			
  			String lastMod = new SimpleDateFormat("yyyyMMddHHmm").format(file.lastModified());
  			
			try {
				lmts = format.parse(lastMod);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			DateTime dt1 = new DateTime(cts);
			DateTime dt2 = new DateTime(lmts);

			int x = Minutes.minutesBetween(dt2, dt1).getMinutes() % 60;
			if ( x >= 1){
				File newFile = new File("/users/ngvinay/bigdata/kafka/data/"+jsonFile);
				System.out.println("txt file renamed to .json :" + file);
				file.renameTo(newFile);
			}
					
		}
    }
	public void json2csv() throws IOException{
		
		String csvFname = null;
		BufferedWriter buffWriter = null;
		StringBuffer sb = new StringBuffer();
		// Append UUID and timestamp to each csv file
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Instant instant = timestamp.toInstant();
		UUID uuid = UUID.randomUUID();
		
		// Filter to list only json files
		FilenameFilter jsonFilter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				String lowercaseName = name.toLowerCase();
				if (lowercaseName.endsWith(".json")) {
					return true;
				} else {
					return false;
				}
			}
			};
			
		// Process all Json files in the directory and convert to csv	
		File[] files = dir.listFiles(jsonFilter);
		int i = 0;
		for (File file : files) {   // Iterate files in dir
			BufferedReader br  = new BufferedReader(new FileReader(file));
			String strLine;
			String[] jsonFname = file.getName().split("_");
		    csvFname = jsonFname[0] + "_" + instant + ".csv";
			
			while((strLine = br.readLine()) != null){ // iterate lines in each file and append to string buffer
				//sb.append(uuid).append(",").append(instant);
				i+=1;
				
				JsonFactory factory = new JsonFactory();
				ObjectMapper mapper = new ObjectMapper(factory);
				JsonNode rootNode = mapper.readTree(strLine);  

				Iterator<Map.Entry<String,JsonNode>> keysIterator = rootNode.fields();
				if ( i == 1) {
					sb.append("uuid").append(",").append("load_ts");
					while (keysIterator.hasNext()) { 
						Map.Entry<String,JsonNode> field = keysIterator.next();
						sb.append(",");
						sb.append(field.getKey());
 	                }
					sb.append("\n");
				}	
								
				Iterator<Map.Entry<String,JsonNode>> fieldsIterator = rootNode.fields();				
				sb.append(uuid).append(",").append(instant);
				while (fieldsIterator.hasNext()) { // Iterate fields in each line
					Map.Entry<String,JsonNode> field = fieldsIterator.next();
					sb.append(",");
					sb.append(field.getValue());
	     
	                }
				sb.append("\n");
			}
			file.delete();
		}
		if( sb.length() != 0){
			//System.out.println(sb);  // Merge multiple json files and write to csv file here
			String filePath = "/Users/ngvinay/bigdata/kafka/data/"+csvFname;
			 buffWriter = new BufferedWriter(new FileWriter(filePath,false));		
			 buffWriter.write(sb.toString());			
			 buffWriter.flush();
			 buffWriter.close();			
		}		
		
		}
		
	}
