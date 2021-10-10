package ops.kafka.serde

import akka.protobuf.InvalidProtocolBufferException
import ops.protocols.kafka.generated.KafkaUserRequest
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaUserRequestDeserializer : KafkaIngressDeserializer<KafkaUserRequest> {
    companion object {
        private const val serialVersionUID: Long = 1L
    }

    override fun deserialize(input: ConsumerRecord<ByteArray, ByteArray>): KafkaUserRequest {
        return try {
            KafkaUserRequest.parseFrom(input.value())
        } catch (ex: InvalidProtocolBufferException) {
            throw RuntimeException(ex)
        }
    }
}
