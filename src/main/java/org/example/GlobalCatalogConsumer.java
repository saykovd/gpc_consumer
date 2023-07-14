package org.example;

import com.betfair.platform.spcs.messages.SportsCatalogProto;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConsumerSeekAware;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalCatalogConsumer extends AbstractConsumerSeekAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalCatalogConsumer.class);


    private Map<TopicPartition, Long> topicPartitionLag= new ConcurrentHashMap<>();;

    private AtomicBoolean hasConsumedAllGPCMessages = new AtomicBoolean();
        int count=0;
    @KafkaListener(topics = "globalProductCatalogueStream",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, SportsCatalogProto.Catalog> consumerRecord, Consumer<String, SportsCatalogProto.Catalog> consumer) {
        LOGGER.info("Offset: {} ; Partition:  {} Count {}", consumerRecord.offset(), consumerRecord.partition(), count++);
        checkIfTopicWasFullyConsumed(consumerRecord, consumer);
        /* if (consumerRecord.value() == null) {
           // LOGGER.info("Invalidating cache for key: {}", consumerRecord.key());
        } else {
            if (consumerRecord.value().getCatalogEntry().getCatalogEntryType().equals("COMPETITIONS")) {
                LOGGER.info("Updating cache for key: {}", consumerRecord.value().getCatalogEntry().getCompetitions().getCompetitionsCount());
            }
        }
       */
    }
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback callback) {
        //Set to beginning
        //callback.seekToBeginning(assignments.keySet());
        //Set to specific time behind current time
        callback.seekToTimestamp(assignments.keySet(), new Date().getTime() - 2*60*1000);
    }

    private void checkIfTopicWasFullyConsumed(ConsumerRecord<String, SportsCatalogProto.Catalog> record, Consumer<String, SportsCatalogProto.Catalog> consumer) {
        if (hasConsumedAllGPCMessages.get()) {
            LOGGER.debug("checkIfTopicWasFullyConsumed: GPC topic already consumed.");
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        long currentLag = consumer.currentLag(topicPartition).orElseGet(() -> calculateConsumerLag(consumer, topicPartition, record));
        topicPartitionLag.put(topicPartition, currentLag);
        hasConsumedAllGPCMessages.set(topicPartitionLag.values().stream()
                .allMatch(lag -> lag == 0));

        if (hasConsumedAllGPCMessages.get()) {
            LOGGER.info("GPC topic was fully consumed.");
        }
    }

    private long calculateConsumerLag(Consumer<String, SportsCatalogProto.Catalog> consumer, TopicPartition topicPartition, ConsumerRecord<String, SportsCatalogProto.Catalog> record) {
        long currentOffset = record.offset();
        long endOffset = consumer.endOffsets(List.of(topicPartition)).get(topicPartition);
        long consumerLag = endOffset - currentOffset;
        LOGGER.debug("calculateConsumerLag: topicPartition[{}], currentOffset[{}], endOffset[{}], consumerLag[{}]", topicPartition, currentOffset, endOffset, consumerLag);

        return consumerLag;
    }
}
