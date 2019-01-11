package de.flaconi.alpha.kafkapoc.config

import de.flaconi.alpha.kafkapoc.model.Data
import de.flaconi.alpha.kafkapoc.model.Result
import de.flaconi.alpha.kafkapoc.serializers.ErrorAwareJsonDeserializer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.support.serializer.JsonSerializer

import java.util.HashMap

@EnableKafka
@Configuration
open class KafkaConfig {

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${spring.kafka.consumer.group-id}")
    private lateinit var consumerGroupId: String

    @Bean
    open fun kafkaProducerFactory(): ProducerFactory<String, Result> {
        val props = HashMap<String, Any>()

        props[BOOTSTRAP_SERVERS_CONFIG] = this.bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java

        return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer())
    }

    @Bean
    open fun kafkaTemplate(): KafkaTemplate<String, Result> {
        return KafkaTemplate(this.kafkaProducerFactory())
    }

    @Bean
    open fun kafkaConsumerFactory(): ConsumerFactory<String, Data> {
        val props = this.consumerProperties()

        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorAwareJsonDeserializer::class.java

        return DefaultKafkaConsumerFactory(
                props,
                StringDeserializer(),
                ErrorAwareJsonDeserializer(Data::class.java))
    }

    @Bean
    open fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Data> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Data>()
        factory.consumerFactory = this.kafkaConsumerFactory()
        return factory
    }

    @Bean
    open fun kafkaConsumer(): Consumer<String, String> {
        return KafkaConsumer(this.consumerProperties())
    }

    private fun consumerProperties(): MutableMap<String, Any> {
        val props = HashMap<String, Any>()

        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroupId
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        return props
    }
}
