package com.kwj.spring.kafka.oncekafkaconsumer.kafka;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InfiniteRecordProducer implements ApplicationRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicLong counter = new AtomicLong();

    private static final long MAX_COUNT = 10000;
    private static final Duration INTERVAL_DURATION = Duration.ofSeconds(1);
    private static final long INTERVAL_IDLE_COUNT = 15;
    private static final Duration INTERVAL_IDLE_DURATION = Duration.ofSeconds(30);

    @Override
    public void run(ApplicationArguments args) {
        while (counter.get() != MAX_COUNT) {
            produceTestRecord();
        }
    }

    private void produceTestRecord() {
        String testRecord = counter.incrementAndGet() + " : " + LocalDateTime.now();
        kafkaTemplate.send(OnceConsumerConstants.TOPIC, testRecord);
        log.info("[PRODUCER] {} record is published", testRecord);
        sleepInterval();
    }

    private void sleepInterval() {
        Duration duration = counter.get() != 0 && counter.get() % INTERVAL_IDLE_COUNT == 0
            ? INTERVAL_IDLE_DURATION
            : INTERVAL_DURATION;

        try {
            log.info("[PRODUCER] sleep {}", duration);
            Thread.sleep(duration.toMillis());
        } catch (Exception ignored) {
        }
    }

}
