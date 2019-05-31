package com.huanfion.kafka.kafkaStreams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext processorContext = null;

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        //处理数据，出现>>>，只取>>>后面的数据
        String input = new String(value);
        if (input.contains(">>>")) {
            input = input.split(">>>")[1].trim();
        }
        processorContext.forward("logProcessor".getBytes(),input.getBytes());
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
