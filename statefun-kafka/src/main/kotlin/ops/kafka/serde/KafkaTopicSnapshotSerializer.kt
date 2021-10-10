package ops.kafka.serde

import ops.protocols.kafka.generated.KafkaTopicSnapshot
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaTopicSnapshotSerializer : KafkaEgressSerializer<KafkaTopicSnapshot> {
    companion object {
        private const val serialVersionUID: Long = 1L
    }

    override fun serialize(topic: KafkaTopicSnapshot): ProducerRecord<ByteArray, ByteArray?> {
        val key = topic.topicName.toByteArray()
        val value: ByteArray? = if (!deletingSnapshot(topic)) {
            topic.toByteArray()
        } else {
            null
        }

        return ProducerRecord("kafka-topic-snapshots", key, value)
    }

    private fun deletingSnapshot(topic: KafkaTopicSnapshot): Boolean =
        topic.lifecycleStatus == KafkaTopicSnapshot.LifecycleStatus.GONE
}
