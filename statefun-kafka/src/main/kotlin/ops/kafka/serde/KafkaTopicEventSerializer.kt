package ops.kafka.serde

import ops.protocols.kafka.generated.KafkaTopicEvent
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaTopicEventSerializer : KafkaEgressSerializer<KafkaTopicEvent> {
    companion object {
        private const val serialVersionUID: Long = 1L
    }

    override fun serialize(topicEvent: KafkaTopicEvent): ProducerRecord<ByteArray, ByteArray> =
        ProducerRecord(
            "kafka-topic-events",
            topicEvent.topicName.toByteArray(),
            topicEvent.toByteArray()
        )
}
