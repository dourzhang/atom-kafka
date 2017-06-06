package com.atom.consumer.infrastructure.stream;

import com.atom.consumer.service.MsgConsumer;
import com.atom.consumer.service.TopicConstant;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Msg Consumer
 * <p>
 * Created by Atom on 2017/5/23.
 */
@Component
public class StreamMsgConsumer implements MsgConsumer {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ObjectMapper objectMapper;

    @Override
    @KafkaListener(topics = TopicConstant.TOPIC)
    public void receivedMsg(String msg) {

        logger.info("接收消息{}", msg);
        String content = null;
        try {
            Map map = objectMapper.readValue(msg, Map.class);
            content = map.get("content").toString();
        } catch (IOException e) {
            logger.error("json解析错误", e);
        }

        logger.info("content:{}", content);
    }

    @KafkaListener(id = "id-one", topics = "streams-wordcount-processor-output", containerFactory = "kafkaListenerContainerFactory")
    public void streamsProcessorOne(List<ConsumerRecord<String, Integer>> records) {

        logger.info("streams-wordcount-processor-output 接收消息:{}", records.size());

        records.forEach(record -> {
            logger.info("offset:{},key:{},value:{}", record.offset(), record.key(), record.value());
        });

    }

    @KafkaListener(id = "id-two", topics = "streams-wordcount-output", containerFactory = "kafkaListenerContainerFactory")
    public void streamsProcessorTwo(String msg) {

        logger.info("streams-wordcount-output 接收消息:{}", msg);

    }

}
