package ops.kafka

import io.kotest.assertions.asClue
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import ops.kafka.harness.Generator
import ops.protocols.kafka.generated.KafkaTopicSnapshot.LifecycleStatus
import org.apache.flink.statefun.testutils.function.FunctionTestHarness
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class KafkaTopicTest {
    lateinit var harness: FunctionTestHarness

    @BeforeEach
    fun before() {
        harness = FunctionTestHarness.test(
            { KafkaTopicFun(Options()) }, KafkaTopicFun.TYPE, "test-topic"
        )
    }

    @Test
    fun `should add a new topic`() {
        harness.invoke(
            Generator.addTopicRequested(
                "test-topic",
                partitionCount = 1,
                replicationFactor = 2,
            )
        )

        harness.getEgress(ModuleIO.KAFKA_TOPIC_EVENT_EGRESS_ID)
            .shouldHaveSize(1)
            .first().asClue {
                it.topicName.shouldBe("test-topic")
                it.hasTopicAdded().shouldBeTrue()
                it.topicAdded.partitionCount.shouldBe(1)
                it.topicAdded.replicationFactor.shouldBe(2)
            }

        harness.getEgress(ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID)
            .shouldHaveSize(1)
            .first().asClue {
                it.topicName.shouldBe("test-topic")
                it.lifecycleStatus.shouldBe(LifecycleStatus.OK)
                it.partitionCount.shouldBe(1)
                it.replicationFactor.shouldBe(2)
            }
    }

    @Test
    fun `should remove a topic`() {
        harness.invoke(Generator.addTopicRequested("test-topic"))
        harness.invoke(Generator.removeTopicRequested("test-topic"))

        harness.getEgress(ModuleIO.KAFKA_TOPIC_EVENT_EGRESS_ID)
            .shouldHaveSize(2)
            .last().asClue {
                it.topicName.shouldBe("test-topic")
                it.hasTopicRemoved().shouldBeTrue()
            }

        harness.getEgress(ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID)
            .shouldHaveSize(2)
            .last().asClue {
                it.topicName.shouldBe("test-topic")
                it.lifecycleStatus.shouldBe(LifecycleStatus.GONE)
            }
    }
}
