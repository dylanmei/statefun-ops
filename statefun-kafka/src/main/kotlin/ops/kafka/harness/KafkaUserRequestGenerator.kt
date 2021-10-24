package ops.kafka.harness

import ops.protocols.kafka.generated.KafkaUserRequest
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KafkaUserRequestGenerator : SerializableSupplier<KafkaUserRequest> {
    companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaUserRequestGenerator::class.java)
    }

    override fun get(): KafkaUserRequest {
        log.debug("Nothing more to generate. Sleeping for 10m...")
        Thread.sleep(600000)

        return Generator.addUserRequested("foo")
    }
}
