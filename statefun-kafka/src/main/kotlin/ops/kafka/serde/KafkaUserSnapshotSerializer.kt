package ops.kafka.serde

import ops.protocols.kafka.generated.KafkaUserSnapshot
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaUserSnapshotSerializer : KafkaEgressSerializer<KafkaUserSnapshot> {
    companion object {
        private const val serialVersionUID: Long = 1L
    }

    override fun serialize(user: KafkaUserSnapshot): ProducerRecord<ByteArray, ByteArray?> {
        val key = user.userName.toByteArray()
        val value: ByteArray? = if (!deletingSnapshot(user)) {
            user.toByteArray()
        } else {
            null
        }

        return ProducerRecord("kafka-user-snapshots", key, value)
    }

    private fun deletingSnapshot(user: KafkaUserSnapshot): Boolean =
        user.lifecycleStatus == KafkaUserSnapshot.LifecycleStatus.GONE
}
