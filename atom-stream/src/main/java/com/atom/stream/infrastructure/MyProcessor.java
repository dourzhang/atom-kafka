package com.atom.stream.infrastructure;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * 处理器
 * <p>
 * Created by Atom on 2017/6/1.
 */
public class MyProcessor implements Processor {

    private ProcessorContext context;
    private KeyValueStore<String, Integer> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(1000);
        this.kvStore = (KeyValueStore) context.getStateStore("Counts");
    }

    @Override
    public void process(Object dummy, Object line) {

        String[] words = ((String) line).toLowerCase().split(" ");
        for (String word : words) {
            Integer oldValue = this.kvStore.get(word);

            if (oldValue == null) {
                this.kvStore.put(word, 1);
            } else {
                this.kvStore.put(word, oldValue + 1);
            }
        }
    }

    @Override
    public void punctuate(long timestamp) {

        KeyValueIterator<String, Integer> iter = this.kvStore.all();
        while (iter.hasNext()) {
            KeyValue entry = iter.next();
            context.forward(entry.key, entry.value.toString());
        }

        iter.close();
        context.commit();
    }

    @Override
    public void close() {
        this.kvStore.close();
    }
}
