package ops.kafka.harness

import ops.protocols.kafka.generated.KafkaTopicRequest
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KafkaTopicRequestGenerator(val intervalMs: Long) : SerializableSupplier<KafkaTopicRequest> {
    companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaTopicRequestGenerator::class.java)
    }

    var alreadyRunning = false
    var requests = mutableListOf(
        Generator.addTopicRequested("hello-stream"),
        Generator.removeTopicRequested("hello-stream"),
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
