package ops.kafka.harness

import ops.kafka.Options
import ops.protocols.kafka.generated.KafkaTopicRequest
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class KafkaTopicRequestSource(
    options: Options
) : FlinkKafkaConsumer<KafkaTopicRequest>(
    "kafka-topic-requests",
    KafkaTopicRequestDeserializationSchema(),
    options.asKafkaProperties(),
) {
    override fun open(configuration: Configuration) {
        setStartFromLatest()
        setCommitOffsetsOnCheckpoints(false)
        super.open(configuration)
    }
}
