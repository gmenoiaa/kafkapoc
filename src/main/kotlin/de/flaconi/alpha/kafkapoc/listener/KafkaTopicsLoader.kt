package de.flaconi.alpha.kafkapoc.listener

import de.flaconi.alpha.kafkapoc.funs.loggerFor
import org.apache.kafka.clients.consumer.Consumer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import kotlin.streams.toList

@Component
class KafkaTopicsLoader @Autowired constructor(private val kafkaConsumer: Consumer<String, String>) {
    private val logger = loggerFor(javaClass)

    @EventListener(ApplicationReadyEvent::class)
    fun loadKafkaTopics() {
        val allTopics = this.kafkaConsumer.listTopics()
        val filteredTopics = allTopics.keys
                .stream()
                .filter { title -> !title.startsWith("_") }
                .toList()
        this.logger.info("Discovered topics: {}", filteredTopics)
    }
}
