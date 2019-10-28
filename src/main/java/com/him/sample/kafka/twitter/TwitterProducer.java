package com.him.sample.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static final String CONSUMER_SECRET = "HAN8wOdSHzNHbOXNmI6UPvAFn936Nhz2uzGU8SDS33oTUkL3Go";
    public static final String TOKEN = "81571848-clukqqumc5VlDFAhmkWXALecdef6hFRAQyFkleB21";
    public static final String SECRET = "Lt9ITpeX8Z9VIjmv6XwjgeCcaZQYpDktdoSDoA4ViZQxy";
    public static final String CONSUMER_KEY = "OTtKWyfUr2SAKX1GxC28OrKdj";

    public static void main(String[] args)  {
        TwitterProducer producer = new TwitterProducer();
        //create twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        Client client = producer.createTwitterClient(msgQueue);
        client.connect();

        //create kafka producer
        KafkaProducer<String,String> kafkaProducer=getKafkaProducer();



        //loop to send tweets to kafka

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(null!=msg) {
                System.out.println(msg);
                ProducerRecord<String, String> record= new ProducerRecord("twitter-topic",null,msg);
                kafkaProducer.send(record);
            }
        }
        System.out.println("App ended");
    }

    private static KafkaProducer<String, String> getKafkaProducer() {
        return new KafkaProducer(getProducerConfig()) ;
    }

    private static Properties getProducerConfig(){
        Properties configs=new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return configs;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("kafka");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
