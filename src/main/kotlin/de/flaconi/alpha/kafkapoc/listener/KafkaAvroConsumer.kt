package de.flaconi.alpha.kafkapoc.listener

import de.flaconi.alpha.kafkapoc.funs.loggerFor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

@Service
class KafkaAvroConsumer {
    private val logger: Logger = loggerFor(javaClass)

    @KafkaListener(topics = ["Kafka_Avro_Example"], containerFactory = "kafkaAvroListenerContainerFactory1")
    fun consume1(@Payload message: ConsumerRecord<String, Any>) {
        logger.info("Consumed avro data from listener 1: ${message.value()}")
    }

    @KafkaListener(topics = ["Kafka_Avro_Example"], containerFactory = "kafkaAvroListenerContainerFactory2")
    fun consume2(@Payload message: ConsumerRecord<String, Any>) {
        logger.info("Consumed avro data from listener 2: ${message.value()}")
    }
}
