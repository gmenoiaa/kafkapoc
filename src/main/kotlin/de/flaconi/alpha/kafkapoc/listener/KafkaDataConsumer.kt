package de.flaconi.alpha.kafkapoc.listener

import de.flaconi.alpha.kafkapoc.funs.loggerFor
import de.flaconi.alpha.kafkapoc.model.Data
import de.flaconi.alpha.kafkapoc.model.Result
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicInteger

@Service
class KafkaDataConsumer @Autowired constructor(private val kafkaTemplate: KafkaTemplate<String, Result>) {
    private val logger = loggerFor(javaClass)
    private val atomicInteger = AtomicInteger()

    @KafkaListener(topics = ["Kafka_Example"], containerFactory = "kafkaListenerContainerFactory")
    fun consume(@Payload message: Data) {
        logger.info("Consumed string data: $message")
        kafkaTemplate.send("Kafka_Result", Result(atomicInteger.addAndGet(1)))
    }
}
