package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;



public class FirstMessage {
    public static void main(String[] args) {
        String topic ="mytopic";
        String brokerurl="localhost:9092";
        Properties properties=new Properties();
        properties.put("bootstrap.servers",brokerurl);
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> p=new KafkaProducer<String,String>(properties);
        for (int i=0;i<1000;i++){
            p.send(new ProducerRecord<>(topic, "mykey" + i, "myvalue" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception!=null){
                        System.out.println("Exception");
                        exception.printStackTrace();
                    }else{
                        System.out.println("sent successfully"+metadata.offset()+"===>"+metadata.partition());
                    }

                }
            });

        }
    }

}
