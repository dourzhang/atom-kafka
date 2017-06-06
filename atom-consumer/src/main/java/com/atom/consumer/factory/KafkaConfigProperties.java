package com.atom.consumer.factory;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * kafka config
 * <p>
 * Created by Atom on 2017/3/29.
 */
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaConfigProperties {

    private String bootstrapServers;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
