package ops.kafka.harness

import ops.protocols.kafka.generated.KafkaTopicDeletePolicy
import ops.protocols.kafka.generated.KafkaTopicRequest
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration

class KafkaTopicRequestGenerator(val intervalMs: Long) : SerializableSupplier<KafkaTopicRequest> {
    companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaTopicRequestGenerator::class.java)
    }

    val topicName = "hello-stream"
    var alreadyRunning = false

    var requests = mutableListOf(
        KafkaTopicRequest.newBuilder()
            .setTopicName(topicName)
            .setAddRequested(
                KafkaTopicRequest.AddRequested.newBuilder()
                    .setPartitionCount(1)
                    .setReplicationFactor(1)
                    .setDeletePolicy(
                        //KafkaTopicDeletePolicy.newBuilder().setNoPolicy(
                        //    KafkaTopicDeletePolicy.NoPolicy.newBuilder()
                        //)
                        KafkaTopicDeletePolicy.newBuilder()
                            .setWaitTime(Duration.ofSeconds(1).toMillis())
                            .setLogSizePolicy(
                                KafkaTopicDeletePolicy.LogSizePolicy.newBuilder()
                                    .setLteSize(0.0)
                            )
                    )
                    .build()
            )
            .build(),
        KafkaTopicRequest.newBuilder()
            .setTopicName(topicName)
            .setRemoveRequested(
                KafkaTopicRequest.RemoveRequested.newBuilder()
                    .build()
            )
            .build(),
    )

    private fun done() = requests.isEmpty()

    override fun get(): KafkaTopicRequest {
        if (done()) {
            log.debug("Nothing more to generate. Sleeping for 10m...")
            Thread.sleep(600000)
        }

        if (alreadyRunning) {
            Thread.sleep(intervalMs)
        }

        return requests.removeAt(0).also {
            alreadyRunning = true
        }
    }
}
