package com.kwj.spring.kafka.oncekafkaconsumer.kafka;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OnceConsumerService {

    private final KafkaListenerEndpointRegistry listenerEndpointRegistry;

    public void startOnceConsumer() {
        findOnceConsumer().start();
        log.info("[CONSUMER] {} is started", OnceConsumerConstants.LISTENER_ID);
    }

    @KafkaListener(
        id = OnceConsumerConstants.LISTENER_ID,
        topics = OnceConsumerConstants.TOPIC,
        autoStartup = "false"
    )
    public void listenMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("[CONSUMER] {} record is consumed", consumerRecord.value());
        acknowledgment.acknowledge();
    }

    @EventListener
    public void listenConsumerIdleEvent(ListenerContainerIdleEvent event) {
        if (StringUtils.startsWith(event.getListenerId(), OnceConsumerConstants.LISTENER_ID)) {
            stopOnceConsumer();
        }
    }

    private void stopOnceConsumer() {
        findOnceConsumer().stop(() -> log.info("[CONSUMER] {} is stopped", OnceConsumerConstants.LISTENER_ID));
    }

    private MessageListenerContainer findOnceConsumer() {
        return Optional.of(OnceConsumerConstants.LISTENER_ID)
            .map(listenerEndpointRegistry::getListenerContainer)
            .orElseThrow(() -> new IllegalStateException("Failed to find once consumer."));
    }

}
