package com.huanfion.kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;


import java.nio.ByteBuffer;
import java.util.*;

/**
 * kafka消费者使用低级API消费数据
 */
public class LowerConsumer {
    private List<String> m_replicaBrokers = new ArrayList<>();

    //消费指定主题，指定分区，指定偏移量数据
    public static void main(String[] args) {
        String topic = "first";//主题
        int partition = 0;//分区
        long offset = 0;//偏移量
        List<String> brokers = new ArrayList<>();
        brokers.add("192.168.106.128");
        brokers.add("192.168.106.130");
        brokers.add("192.168.106.132");
        //端口号
        int port = 9092;
        LowerConsumer lowerConsumer = new LowerConsumer();
        try {
            lowerConsumer.run(brokers, port, topic, partition, offset);
        } catch (Exception ex) {
            System.out.println("Oops:" + ex);
            ex.printStackTrace();

        }
    }

    public void run(List<String> brokers, int port, String topic, int paritition, long offset) throws Exception {
        PartitionMetadata partitionMetadata = getLeader(brokers, port, topic, paritition);
        if (partitionMetadata == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        if (partitionMetadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = partitionMetadata.leader().host();
        String clientName = "Client_" + topic + "_" + paritition;
        SimpleConsumer consumer = new SimpleConsumer(partitionMetadata.leader().host(), port, 1000, 1024 * 64, clientName);

        //long readOffset = getLastOffset(consumer, topic, paritition, )
        //拉取数据请求
        FetchRequest fetchRequest = new FetchRequestBuilder().clientId(clientName).addFetch(topic, paritition, offset, 1000000).build();
        //拉取数据
        FetchResponse fetchResponse = consumer.fetch(fetchRequest);
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, paritition);
        //打印数据
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println("offset:" + messageAndOffset.offset() + "---" + "data:" + new String(bytes, "UTF-8"));

        }
        consumer.close();
    }

    //读取起始偏移量
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition,PartitionOffsetRequestInfo> requestInfo=new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition,new PartitionOffsetRequestInfo(whichTime,1));
        OffsetRequest request=new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),clientName);
        OffsetResponse offsetsBefore = consumer.getOffsetsBefore(request);
        if(offsetsBefore.hasError()){
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + offsetsBefore.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = offsetsBefore.offsets(topic, partition);
        return  offsets[0];
    }

    //获取topic分区leader
    public PartitionMetadata getLeader(List<String> brokers, int port, String topic, int paritition) {
        PartitionMetadata retrunData = null;
        loop:
        for (String broker : brokers) {
            SimpleConsumer consumer = null;
            try {

                consumer = new SimpleConsumer(broker, port, 1000, 1024 * 64, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                //获取topic元数据信息
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metadata = resp.topicsMetadata();

                for (TopicMetadata item : metadata) {
                    //获取一个topic中所有的分区元数据信息
                    List<PartitionMetadata> partitionMetadata = item.partitionsMetadata();
                    for (PartitionMetadata part : partitionMetadata) {
                        if (part.partitionId() == paritition) {
                            retrunData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception ex) {
                System.out.println("Error communicating with Broker [" + broker + "] to find Leader for [" + topic + ", " + paritition + "] Reason: " + ex);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        if (retrunData != null) {
            m_replicaBrokers.clear();
            for (BrokerEndPoint replica : retrunData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return retrunData;
    }

    //获取新的leader
    public String findNewLeader(String oldLeader, int port, String topic, int partition) throws Exception {
        for (int i = 0; i < 3; i++) {
            PartitionMetadata partitionMetadata = getLeader(m_replicaBrokers, port, topic, partition);
            boolean goToSleep = false;
            if (partitionMetadata == null) {
                goToSleep = true;
            } else if (partitionMetadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(partitionMetadata.leader().host()) && i == 0) {
                goToSleep = true;
            } else {
                return partitionMetadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {

                }
            }
        }
        System.out.println("broker报错后没有找到新的leader");
        throw new Exception("broker报错后没有找到新的leader");
    }
}
