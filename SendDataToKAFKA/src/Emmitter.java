import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;


public class Emmitter extends Thread{

	Producer producer;
	org.apache.kafka.clients.producer.Producer<String, String> prod;
	
	public Emmitter(Producer producer) {
		this.producer=producer;
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prod=new KafkaProducer<String, String>(props);
	}
	
	
	@Override
	public void run() {

		super.run();
		try {
			while(scheduleDataTransfer()==true){
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	
	boolean scheduleDataTransfer() throws IOException, JSONException, InterruptedException{
		JSONObject data=null;
		int secPrev=0,secCur;
		int waitTime;
		
		while((data=producer.getData())!=null){
			String time=(String) data.get("ticket time");
			secCur=Integer.parseInt(time.split(":")[2]);
			if(secPrev>secCur){
				waitTime=secCur+60-secPrev;
			}else{
				waitTime=secCur-secPrev;
			}
			sleep(waitTime*1000);
			
			sendData(data.toString());
			return true;
		}
		return false;
	}
	
	void sendData(String data){
		prod.send(new ProducerRecord<String, String>("data", data));	
	}
	
}
