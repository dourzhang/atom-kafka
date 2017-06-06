package com.atom.producer.infrastructure.stream;

import com.atom.producer.domain.service.MsgProducerService;
import com.atom.producer.domain.service.TopicConstant;
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

@Component("streamMsgProducer")
public class StreamMsgProducer implements MsgProducerService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private KafkaTemplate kafkaTemplate;

    @Override
    public void sendMsg() {

        String type = TopicConstant.STREAM_FILE;
        Map<String, Object> map = new HashMap<>();
        Long count = 0L;
        while (count < 3L) {
            map.put(count.toString(), "hello" + count);
            count++;
        }
        this.send(type, map);
    }

    private void send(String type, Map<String, Object> map) {
        logger.info("type:{},map:{}", type, map);
        map.forEach((k, v) -> kafkaTemplate.send(type, k, v));
    }

}
