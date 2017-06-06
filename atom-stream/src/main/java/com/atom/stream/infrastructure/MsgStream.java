package com.atom.stream.infrastructure;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;

/**
 * Msg Stream
 * <p>
 * Created by Atom on 2017/5/27.
 */
@Component
public class MsgStream {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public void msgHandle() {


    }
}
