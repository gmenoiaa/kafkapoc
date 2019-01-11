package de.flaconi.alpha.kafkapoc.serializers

import org.apache.kafka.common.header.Headers
import org.springframework.kafka.support.serializer.JsonDeserializer

class ErrorAwareJsonDeserializer<T> internal constructor(tClass: Class<T>) : JsonDeserializer<T>(tClass) {

    override fun deserialize(topic: String?, headers: Headers, data: ByteArray?): T? {
        return try {
            super.deserialize(topic, data)
        } catch (e: Exception) {
            null
        }
    }
}
