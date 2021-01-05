package com.rvaid.kafka.prodcon;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
        private static ObjectMapper mapper = new ObjectMapper();
        public static RestHighLevelClient createClient(){
                String hostname = "";
                String username = "";
                String password = "";

                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(username, password));

                RestClientBuilder builder = RestClient.builder(
                        new HttpHost(hostname, 443, "https")
                ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                });

                RestHighLevelClient client = new RestHighLevelClient(builder);
                return client;
        }

        public static KafkaConsumer<String,String> createConsumer(String topic){
                Properties properties = new Properties();

                properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.put(ConsumerConfig.GROUP_ID_CONFIG, "twitter-gp");
                properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

                // Create the consumer
                KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
                // Subscribe consumer to our topic
                consumer.subscribe(Arrays.asList(topic));
                return consumer;
        }

        private static String extractIdFromTweet(String tweet){

                JsonNode node = null;
                try {
                        node = mapper.readValue(tweet, JsonNode.class);
                } catch (JsonProcessingException e) {
                        e.printStackTrace();
                }
                return node.get("id_str").textValue();
        }

        public static void main(String[] args) throws IOException {
                Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
                RestHighLevelClient client = createClient();

                KafkaConsumer<String, String> consumer = createConsumer("twitter-tweets");

                // Poll for new data
                while (true){
                        ConsumerRecords<String, String> consumerRecords =
                                consumer.poll(Duration.ofMillis(100));
                        logger.info("Received " + consumerRecords.count() + " records");
                        for (ConsumerRecord record : consumerRecords){
                                // To make the consumer idempotent
//                                // Kafka generic id
//                                String id = record.topic() + "-" + record.partition() + "-" + record.offset();

                                // id from twitter feed
                                String id = extractIdFromTweet(record.value().toString());
                                IndexRequest indexRequest = new IndexRequest("twitter").id(id)
                                        .source(record.value().toString(), XContentType.JSON);

                                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                                logger.info(indexResponse.getId());

                                try {
                                        Thread.sleep(10);
                                } catch (InterruptedException e) {
                                        e.printStackTrace();
                                }
                        }
                        logger.info("Committing offsets...");
                        consumer.commitSync();
                        logger.info("Offsets committed");
                        try {
                                Thread.sleep(1000);
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }

                }

//                client.close();
        }

}
