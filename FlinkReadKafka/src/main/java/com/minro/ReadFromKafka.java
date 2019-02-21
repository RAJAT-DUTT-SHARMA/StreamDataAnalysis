package com.minro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.json.JSONObject;

public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer011<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));
		// print() will write the contents of the stream to the TaskManager's standard out stream
		// the rebalance call is causing a repartitioning of the data so that all machines
		// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
		
		DataStream<Pojo> jsonDataStream=messageStream.map(new MapFunction<String, Pojo>() {
			@Override
			public Pojo map(String value) throws Exception {
				// TODO Auto-generated method stub
				JSONObject object=new JSONObject(value);
				Pojo pojo=new Pojo();
				pojo.setSpeed(object.getInt("speed"));
				pojo.setTimestamp(object.getString("timestamp"));
				pojo.setVehicle_number(object.getString("vehicle_number"));
				return pojo; 
			}
		});
		
		
		WindowedStream<Pojo,Tuple,GlobalWindow> windowStream=jsonDataStream.keyBy("vehicle_number").countWindow(10,3);
		SingleOutputStreamOperator<Pojo> stream=windowStream.sum("speed");
		stream.map(new MapFunction<Pojo, String>() {
			@Override
			public String map(Pojo value) throws Exception {
				// TODO Auto-generated method stub
				return " Average Speed of "+value.vehicle_number +" = "+(value.speed/10);
			}
		}).print();
		env.execute();
	}
}


