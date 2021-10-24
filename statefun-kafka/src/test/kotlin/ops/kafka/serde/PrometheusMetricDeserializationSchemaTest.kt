package ops.kafka.serde

import io.kotest.assertions.asClue
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import ops.protocols.prometheus.generated.PrometheusLabel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test

class PrometheusMetricDeserializationSchemaTest {
    @Test
    fun `should deserialize prometheus metrics`() {
        val metricString = """
        {
            "name": "test_metric",
            "labels": {
                "__name__": "test_metric",
                "test_label": "test-label"
            },
            "timestamp": "2021-10-24T13:30:54Z",
            "value": "123"
        }
        """.trimIndent()

        val schema = PrometheusMetricDeserializationSchema()
        schema.deserialize(consumerRecord(metricString))
            .shouldNotBeNull()
            .asClue {
                it.name.shouldBe("test_metric")
                it.labelsList.shouldContainExactly(
                    PrometheusLabel.newBuilder()
                        .setName("test_label")
                        .setValue("test-label")
                        .build()
                )
                it.timestamp.shouldBe(1635082254000L)
                it.value.shouldBe(123.0)
            }
    }

    private fun consumerRecord(metricString: String): ConsumerRecord<ByteArray, ByteArray> =
        ConsumerRecord(
            "prometheus-metrics",
            0,
            0,
            null,
            metricString.toByteArray(Charsets.UTF_8),
        )
}
