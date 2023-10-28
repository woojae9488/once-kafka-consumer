package com.kwj.spring.kafka.oncekafkaconsumer.kafka;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@Configuration
@RequiredArgsConstructor
public class EmbeddedKafkaConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public EmbeddedKafkaBroker embeddedKafkaBroker() {
        List<String> bootstrapServers = kafkaProperties.getConsumer().getBootstrapServers();

        return new EmbeddedKafkaBroker(1, false, 3, OnceConsumerConstants.TOPIC)
            .brokerProperty("listeners", "PLAINTEXT://" + StringUtils.join(bootstrapServers, ","));
    }

    @Bean
    public ProducerFactory<String, String> producerFactory(EmbeddedKafkaBroker embeddedKafkaBroker) {
        return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
