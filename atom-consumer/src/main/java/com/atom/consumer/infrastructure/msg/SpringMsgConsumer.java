package com.atom.consumer.infrastructure.msg;

import com.atom.consumer.service.MsgConsumer;
import com.atom.consumer.service.TopicConstant;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

/**
 * Msg Consumer
 * <p>
 * Created by Atom on 2017/5/23.
 */
@Component
public class SpringMsgConsumer implements MsgConsumer {

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

    @KafkaListener(id = "id-spring", topics = TopicConstant.TOPIC, containerFactory = "kafkaListenerContainerFactory")
    public void kafkaListenerContainerFactoryReceivedMsg(String msg) {

        logger.info("kafkaListenerContainerFactory 接收消息:{}", msg);
        String content = null;
        try {
            Map map = objectMapper.readValue(msg, Map.class);
            content = map.get("content").toString();
        } catch (IOException e) {
            logger.error("json解析错误", e);
        }
        logger.info("kafkaListenerContainerFactory content:{}", content);
    }

}
