package com.atom.consumer.factory;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * kafka config
 * <p>
 * Created by Atom on 2017/3/29.
 */
@ConfigurationProperties(prefix = "spring.kafka.consumer")
public class KafkaConfigConsumerProperties {

    private String groupId;


    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

}
