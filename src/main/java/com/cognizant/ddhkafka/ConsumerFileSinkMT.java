package com.cognizant.ddhkafka;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerFileSinkMT {
   public static void main(String[] args) throws Exception {
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
       
      }
      
      //Get topic name from console
      final String topicName = args[0].toString();
      
      //Kafka consumer configuration settings
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "localhost:9092"); // Kafka server running at port 9092 on localhost
      props.put("group.id", "test");                    // consumer group test
      props.put("enable.auto.commit", "true");          // Messages will be auto committed
      props.put("auto.commit.interval.ms", "1000");     
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      final KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      //Counsumer subscribes to the given list of topics
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);
      int i = 0;
      
      final ExecutorService executorService = Executors.newFixedThreadPool(2);
      
      executorService.execute(new Runnable() {
        public void run() {
        	 try{ 
        		 while (true) {
           		 ConsumerRecords<String, String> records = consumer.poll(500);
                
           		 if(records.count() != 0) {
           			 BufferedWriter buffWriter = null;
					for (ConsumerRecord<String, String> record : records){
           				 // print the offset,key and value for the consumer records.
           				 System.out.printf("offset = %d, key = %s, value = %s\n", 
               				 			record.offset(), record.key(), record.value());
           				 String currentTs = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
           				 String fileName = topicName + "_" + currentTs + ".txt";
           				 String filePath = "/Users/ngvinay/bigdata/kafka/data/"+fileName;
           				 try {
							buffWriter = new BufferedWriter(new FileWriter(filePath,true));
							buffWriter.write(record.value() + System.lineSeparator());
	           				buffWriter.flush();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
           			 }
           			 buffWriter.close();
           		 }
                }
        	 }
        	 catch (IOException e) {
     			e.printStackTrace();
     		}
        }
     });
      // Run filecheck and convert to csv simultaneously on a separate thread.
      executorService.execute(new Runnable() {
          public void run() {
        	  FileWatcher fw = new FileWatcher();
        	  while(true){      		  
        		  try {
        			fw.checkFiles();
					fw.json2csv();
				} catch (IOException e) {
					
					e.printStackTrace();
				}
        		  
        	  }	   
          } 
       });
      
      //To catch ctrl +c interruptions and shutdown executorService
      Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
          @Override
          public void run() {
          	executorService.shutdown();
              
          }
      });
   }
 }
 

   
 



