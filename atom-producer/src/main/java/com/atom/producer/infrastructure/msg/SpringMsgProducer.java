package com.atom.producer.infrastructure.msg;

import com.atom.producer.domain.service.MsgProducerService;
import com.atom.producer.domain.service.TopicConstant;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * 消息生产者
 * <p>
 * Created by Atom on 2017/5/23.
 */

@Component
public class SpringMsgProducer implements MsgProducerService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private KafkaTemplate kafkaTemplate;

    @Override
    public void sendMsg() {

        String type = TopicConstant.TOPIC;
        Map<String, Object> map = new HashMap<>();
        //map.put("content", "I'm a msg!");
        Long count = 0L;
        while (count <= 3L) {
            map.put("content", "I'm a msg!" + count++);
            this.send(type, map);
        }
    }

    private void send(String type, Map<String, Object> map) {
        logger.info("type:{},content:{}", type, map);
        try {
            kafkaTemplate.send(type, objectMapper.writeValueAsString(map));
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
