package com.mycompany.app;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import schema.Customer;
import common.SerdeGenerator;

public class KStreamFilter {
  
    final static String topic = "customer";
    
    final static String filteredTopic = "taku-age-filtered";

    protected Logger logger = Logger.getLogger(this.getClass().getName());
    
    public KStreamFilter() {

        logger.info("KStreamFilter Starting");
        
        try {
            // Get stream config
            Properties props = getStreamConfig();

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            Serde<Integer> intSerde = Serdes.Integer();
            SpecificAvroSerde<Customer> valueSerde = SerdeGenerator.getSerde(props);
            
            // Define the processing topology of the Streams application.
            final StreamsBuilder builder = new StreamsBuilder();
            Consumed<Integer, Customer> consumed = Consumed.with(intSerde, valueSerde);
            KStream<Integer, Customer> allRecords = builder.stream(topic, consumed);
            Produced<Integer, Customer> ageFilterProduced = Produced.with(intSerde, valueSerde);
            KStream<Integer, Customer> ageFiltered = builder.stream(filteredTopic, consumed);

            allRecords
                .filter((key, customer) -> {
                    Integer age = customer.getAge();
                    return age >= 30 && age <= 50;
                })
                .to(
                    filteredTopic, 
                    ageFilterProduced
                );

            ageFiltered
                .peek((k, v) -> 
                    System.out.println("Filtered key:" + k + ", value: " + v)
                );

            final KafkaStreams streams = new KafkaStreams(builder.build(), props);

            streams.cleanUp();

            streams.start();

            // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private Properties getStreamConfig() throws IOException {
        Properties conf = new Properties();
        conf.load(this.getClass().getResourceAsStream("/client.properties"));
        return conf;
    }

    public static void main(String[] args) {
        System.out.print("Hello World33");
        try {
            new KStreamFilter();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
