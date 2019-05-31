package com.huanfion.kafka.kafkaStreams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopologyBuilder;


/*
kafka stream 数据清洗案例
 */
public class KafkaStreamApp {
    public static void main(String[] args) {
        //定义topic
        String from = "fisrt";
        String to = "second";
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTimeExtractor.class.getName());
        StreamsConfig config = new StreamsConfig(settings);

        //创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();
        //构建拓扑
        builder.addSource("source", "first").addProcessor("processor", (ProcessorSupplier<byte[], byte[]>) LogProcessor::new
                , "source").addSink("sink", "second", "processor");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
    }
}
