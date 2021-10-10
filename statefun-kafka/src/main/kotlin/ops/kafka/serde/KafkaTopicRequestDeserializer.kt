package ops.kafka.serde

import akka.protobuf.InvalidProtocolBufferException
import ops.protocols.kafka.generated.KafkaTopicRequest
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaTopicRequestDeserializer : KafkaIngressDeserializer<KafkaTopicRequest> {
    companion object {
        private const val serialVersionUID: Long = 1L
    }

    override fun deserialize(input: ConsumerRecord<ByteArray, ByteArray>): KafkaTopicRequest {
        return try {
            KafkaTopicRequest.parseFrom(input.value())
        } catch (ex: InvalidProtocolBufferException) {
            throw RuntimeException(ex)
        }
    }
}
