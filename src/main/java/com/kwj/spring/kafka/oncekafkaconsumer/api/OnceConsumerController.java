package com.kwj.spring.kafka.oncekafkaconsumer.api;

import com.kwj.spring.kafka.oncekafkaconsumer.kafka.OnceConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/kafka/once-consumer")
@RequiredArgsConstructor
public class OnceConsumerController {

    private final OnceConsumerService consumerService;

    @PostMapping("/start")
    public void startOnceConsumer() {
        consumerService.startOnceConsumer();
    }

}
