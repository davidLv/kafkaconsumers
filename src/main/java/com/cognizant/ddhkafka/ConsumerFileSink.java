package com.cognizant.ddhkafka;

import java.util.Properties;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerFileSink {
   public static void main(String[] args) throws Exception {
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      //Kafka consumer configuration settings
      String topicName = args[0].toString();
      Properties props = new Properties();
      
      
      BufferedWriter buffWriter = null;
      
      /*try {
          buffWriter = new BufferedWriter(new FileWriter(filePath));
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } */
      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);
      int i = 0;
      
      
      while (true) {
    	 try{ 
    		 
         ConsumerRecords<String, String> records = consumer.poll(500);
         for (ConsumerRecord<String, String> record : records){
         // print the offset,key and value for the consumer records.
        	 System.out.printf("offset = %d, key = %s, value = %s\n", 
        			 record.offset(), record.key(), record.value());
        	 if(record.value() != null) 
        	 {
        		 String currentTs = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
        		 String fileName = topicName + "_" + currentTs + ".txt";
        		 String filePath = "/Users/ngvinay/bigdata/kafka/data/"+fileName;
        		 buffWriter = new BufferedWriter(new FileWriter(filePath));
        		 buffWriter.write(record.value() + System.lineSeparator());
        		 buffWriter.flush();
        		 buffWriter.close();
        	 }
         }
             
    	 }
    	 catch (IOException e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
         }
      }
   }
}

