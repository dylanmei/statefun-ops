package ops.kafka.serde

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import ops.protocols.prometheus.generated.PrometheusLabel
import ops.protocols.prometheus.generated.PrometheusMetric
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Instant

class PrometheusMetricDeserializationSchema :
    KafkaDeserializationSchema<PrometheusMetric> {

    override fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>): PrometheusMetric {
        val body = String(record.value(), Charsets.UTF_8)
        val json = Json {
            isLenient = true
            ignoreUnknownKeys = true
            allowSpecialFloatingPointValues = true
        }.decodeFromString<JsonMetric>(body)

        return PrometheusMetric.newBuilder()
            .setName(json.name)
            .setTimestamp(
                Instant.parse(json.timestamp).toEpochMilli()
            )
            .setValue(json.value)
            .addAllLabels(
                json.labels.filterNot {
                    it.key == "__name__"
                }.map {
                    PrometheusLabel.newBuilder()
                        .setName(it.key)
                        .setValue(it.value)
                        .build()
                }
            )
            .build()
    }

    override fun isEndOfStream(metric: PrometheusMetric?) = false

    override fun getProducedType(): TypeInformation<PrometheusMetric> =
        TypeExtractor.getForClass(PrometheusMetric::class.java)

    @Serializable
    data class JsonMetric(
        val name: String,
        val timestamp: String,
        val value: Double,
        val labels: Map<String, String>,
    )
}
