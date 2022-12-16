package com.mycompany.app;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import schema.Customer;
import common.SerdeGenerator;

public class KStreamGroupByCount {
  
    final static String topic = "customer";
    
    final static String outputTopic = "taku-region-count";

    protected Logger logger = Logger.getLogger(this.getClass().getName());
    
    public KStreamGroupByCount() {

        logger.info("KStreamGroupByCount Starting");
        
        try {
            // Get stream config
            Properties props = getStreamConfig();

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            Serde<Integer> intSerde = Serdes.Integer();
            Serde<String> strSerde = Serdes.String();
            Serde<Long> longSerde = Serdes.Long();
            SpecificAvroSerde<Customer> customerSerde = SerdeGenerator.getSerde(props);
            
            // Define the processing topology of the Streams application.
            final StreamsBuilder builder = new StreamsBuilder();
            Consumed<Integer, Customer> consumed = Consumed.with(intSerde, customerSerde);
            // KStream<Integer, Customer> allRecords = builder.stream(topic, consumed);

            Consumed<String, Long> regionCountConsumed = Consumed.with(strSerde, longSerde);
            Produced<String, Long> regionCountProduced = Produced.with(strSerde, longSerde);
            KTable<String, Long> regionCountRecords = builder.table(outputTopic, regionCountConsumed);
            
            
            builder
                .stream(topic, consumed)
                .map((id, customer) -> new KeyValue<>(customer.getRegion(), 1))
                .groupByKey(Grouped.with(strSerde, intSerde))
                .count()
                .toStream()
                // .peek((region, count) -> System.out.println("allRecords key:" + region + ", value: " + count))
                .to(
                    outputTopic,
                    regionCountProduced
                );

            regionCountRecords
                .toStream()
                .peek((region, count) -> System.out.println("Region:" + region + ", count: " + count));

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
        try {
            new KStreamGroupByCount();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
