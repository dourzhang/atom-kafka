package com.atom.producer.infrastructure.msg;

import com.atom.producer.domain.service.MsgProducerService;
import com.atom.producer.domain.service.TopicConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * KafkaMsg Producer
 * <p>
 * Created by Atom on 2017/5/27.
 */
@Component
public class KafkaMsgProducer implements MsgProducerService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void sendMsg() {

        String type = TopicConstant.TOPIC;
        Map<String, String> map = new HashMap<>();
        //map.put("content", "I'm a msg!");
        Long count = 0L;
        while (count <= 100L) {
            map.put("content", "I'm a msg!" + count++);
            this.send(type, map);
        }
    }

    private void send(String topic, Map<String, String> dataMap) {

        //http://kafka.apache.org/documentation.html#producerconfigs
        Properties props = this.getBaseConfig();

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        dataMap.forEach((k, v) ->
                producer.send(new ProducerRecord<>(topic, k, v))
        );
        producer.close();
    }

    private void sendByteNoBlock(String topic, Map<byte[], byte[]> dataMap) {

        Properties props = this.getBaseConfig();
        props.put("key.serializer", "org.apache.kafka.common.serialization.BytesSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.BytesSerializer");

        dataMap.forEach((k, v) -> {
            ProducerRecord<byte[], byte[]> myRecord = new ProducerRecord<>(topic, k, v);
            Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
            producer.send(myRecord,
                    (metadata, e) -> {
                        if (e != null) {
                            e.printStackTrace();
                            return;
                        }
                        logger.info("The offset of the record we just sent is:{} ", metadata.offset());
                    });
        });
    }

    private void sendByteBlock(String topic, Map<byte[], byte[]> dataMap) {

        Properties props = this.getBaseConfig();
        props.put("key.serializer", "org.apache.kafka.common.serialization.BytesSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.BytesSerializer");

        Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
        dataMap.forEach((k, v) -> {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, k, v);
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private Properties getBaseConfig() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        return props;
    }

}
