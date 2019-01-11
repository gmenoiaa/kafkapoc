package de.flaconi.alpha.kafkapoc.config

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@EnableKafka
@Configuration
open class KafkaAvroConfig {

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${spring.kafka.consumer.group-id}")
    private lateinit var consumerGroupId: String

    @Value("\${spring.kafka.schema-registry-url}")
    private lateinit var schemaRegistryUrl: String

    @Bean
    open fun kafkaAvroListenerContainerFactory1(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = this.kafkaAvroConsumerFactory(this.consumerGroupId + "1")
        return factory
    }

    @Bean
    open fun kafkaAvroListenerContainerFactory2(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = this.kafkaAvroConsumerFactory(this.consumerGroupId + "2")
        return factory
    }

    @Bean
    open fun schemaRegistryClient(): SchemaRegistryClient {
        return CachedSchemaRegistryClient(this.schemaRegistryUrl, 10)
    }

    private fun consumerProperties(consumerGroupId: String): MutableMap<String, Any> {
        val props = HashMap<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = this.bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroupId
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::javaClass
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::javaClass
        props[SCHEMA_REGISTRY_URL] = this.schemaRegistryUrl
        return props
    }

    private fun kafkaAvroConsumerFactory(consumerGroupId: String): ConsumerFactory<String, Any> {
        return DefaultKafkaConsumerFactory(
                this.consumerProperties(consumerGroupId),
                StringDeserializer(),
                KafkaAvroDeserializer(this.schemaRegistryClient()))
    }

    companion object {
        private const val SCHEMA_REGISTRY_URL = "schema.registry.url"
    }
}
