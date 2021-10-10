package ops.kafka.harness

import ops.protocols.kafka.generated.KafkaTopicRequest
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaTopicRequestDeserializationSchema :
    KafkaDeserializationSchema<KafkaTopicRequest> {

    override fun getProducedType(): TypeInformation<KafkaTopicRequest> =
        TypeExtractor.getForClass(KafkaTopicRequest::class.java)

    override fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>): KafkaTopicRequest =
        KafkaTopicRequest.parseFrom(record.value())

    override fun isEndOfStream(m: KafkaTopicRequest) = false
}
