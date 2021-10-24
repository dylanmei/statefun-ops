package ops.kafka

import io.kotest.assertions.asClue
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import ops.kafka.harness.Generator
import ops.protocols.kafka.generated.KafkaUserSnapshot.LifecycleStatus
import org.apache.flink.statefun.testutils.function.FunctionTestHarness
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

class KafkaUserTest {
    lateinit var harness: FunctionTestHarness

    @BeforeEach
    fun before() {
        harness = FunctionTestHarness.test(
            { KafkaUserFun(Options()) }, KafkaUserFun.TYPE, "test-user"
        )
    }

    @Test
    fun `should add a new user`() {
        harness.invoke(
            Generator.addUserRequested(
                "test-user",
                mapOf(
                    "consumer_byte_rate" to 123L,
                    "producer_byte_rate" to 456L,
                )
            )
        )

        harness.getEgress(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID)
            .shouldHaveSize(1)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.hasUserAdded().shouldBeTrue()
                it.userAdded.quotasMap.shouldContainExactly(
                    mapOf(
                        "consumer_byte_rate" to 123L,
                        "producer_byte_rate" to 456L,
                    )
                )
            }

        harness.getEgress(ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID)
            .shouldHaveSize(1)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.lifecycleStatus.shouldBe(LifecycleStatus.OK)
                it.quotasMap.shouldContainExactly(
                    mapOf(
                        "consumer_byte_rate" to 123L,
                        "producer_byte_rate" to 456L,
                    )
                )
            }
    }

    @Test
    fun `should remove a user`() {
        harness.invoke(Generator.addUserRequested("test-user"))
        harness.invoke(Generator.removeUserRequested("test-user"))

        harness.getEgress(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID)
            .shouldHaveSize(2)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.hasUserRemoved().shouldBeTrue()
            }

        harness.getEgress(ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID)
            .shouldHaveSize(2)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.lifecycleStatus.shouldBe(LifecycleStatus.GONE)
            }
    }

    @Test
    fun `should add a credential`() {
        harness.invoke(Generator.addUserRequested("test-user"))
        harness.invoke(
            Generator.addCredentialRequested(
                "test-user",
                "test-credential",
                "test-secret",
                Instant.parse("2999-01-01T00:00:00Z")
            )
        )

        harness.getEgress(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID)
            .shouldHaveSize(2)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.hasCredentialAdded().shouldBeTrue()
                it.credentialAdded.identifier.shouldBe("test-credential")
                it.credentialAdded.secretHash.shouldBe("test-secret".sha256())
                it.credentialAdded.expirationTime.shouldBe(32472144000000L)
            }

        harness.getEgress(ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID)
            .shouldHaveSize(2)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.lifecycleStatus.shouldBe(LifecycleStatus.OK)
                it.credentialsList.shouldHaveSize(1)
                it.credentialsList.first().asClue { c ->
                    c.identifier.shouldBe("test-credential")
                    c.secretHash.shouldBe("test-secret".sha256())
                    c.expirationTime.shouldBe(32472144000000L)
                }
            }
    }

    @Test
    fun `should revoke a credential`() {
        harness.invoke(Generator.addUserRequested("test-user"))
        harness.invoke(Generator.addCredentialRequested("test-user", "test-credential"))
        harness.invoke(Generator.revokeCredentialRequested("test-user", "test-credential"))

        harness.getEgress(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID)
            .shouldHaveSize(3)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.hasCredentialRevoked().shouldBeTrue()
                it.credentialRevoked.identifier.shouldBe("test-credential")
            }

        harness.getEgress(ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID)
            .shouldHaveSize(3)
            .last().asClue {
                it.userName.shouldBe("test-user")
                it.lifecycleStatus.shouldBe(LifecycleStatus.OK)
                it.credentialsList.shouldHaveSize(0)
            }
    }
}
