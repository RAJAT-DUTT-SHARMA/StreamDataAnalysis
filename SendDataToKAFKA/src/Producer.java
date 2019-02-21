import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONException;
import org.json.JSONObject;


public class Producer extends Thread {
	
	BufferedReader reader;
	String colNames[];
	
	int threshold=20;
	
	JSONObject dataBuffer[];
	
	AtomicInteger bufferPointer; 
	
	@Override
	public void run() {
		while(bufferPointer.get()<50){
			try {
				readDataFromFile();
			} catch (Exception e) {
				break;
			}
		}
	}
	
	public Producer() {
		String filename="/home/rajatds/Desktop/bmtc data/etim_gprs_ticket_08012017-000.csv";
		try {
			
			bufferPointer=new AtomicInteger(-1);
			dataBuffer=new JSONObject[50];
			reader=new BufferedReader(new FileReader(new File(filename)));
			colNames=reader.readLine().split(",");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	boolean readDataFromFile() throws IOException, JSONException{
		String line ;
		line=reader.readLine();
		if(line!=null){
			String []columns=line.split(",");
			int i=0;
			JSONObject object=new JSONObject();
			for(String colName:colNames){
				object.put(colName, columns[i]);
				i++;
			}
			synchronized (dataBuffer) {
				dataBuffer[bufferPointer.incrementAndGet()]=object;
			}
			return true;
		}else{
			reader.close();
			return false;
		}
	}
	
	public JSONObject getData(){
		JSONObject object;
		synchronized (dataBuffer) {
			object= dataBuffer[bufferPointer.getAndDecrement()];	
		}	
		if(bufferPointer.get()<threshold){
		run();
		}
		return object;
	}
	
	
}
