package ops.kafka.serde

import ops.protocols.kafka.generated.KafkaUserEvent
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaUserEventSerializer : KafkaEgressSerializer<KafkaUserEvent> {
    companion object {
        private const val serialVersionUID: Long = 1L
    }

    override fun serialize(userEvent: KafkaUserEvent): ProducerRecord<ByteArray, ByteArray> =
        ProducerRecord(
            "kafka-user-events",
            userEvent.userName.toByteArray(),
            userEvent.toByteArray()
        )
}
