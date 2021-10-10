package ops.kafka

import ops.protocols.prometheus.generated.PrometheusMetric
import org.apache.flink.statefun.sdk.Address
import org.apache.flink.statefun.sdk.Context
import org.apache.flink.statefun.sdk.FunctionType
import org.apache.flink.statefun.sdk.StatefulFunction
import org.apache.flink.statefun.sdk.annotations.Persisted
import org.apache.flink.statefun.sdk.state.Expiration
import org.apache.flink.statefun.sdk.state.PersistedValue
import java.time.Duration

class MetricFun : StatefulFunction {
    companion object {
        val TYPE = FunctionType(ModuleIO.FUNCTION_NAMESPACE, "prometheus-metric")
        private fun splitId(address: Address): String = address.id().split(":").last()
    }

    @Persisted
    private val metricState = PersistedValue.of(
        "metric",
        Double::class.java,
        Expiration.expireAfterWriting(Duration.ofDays(1))
    )

    override fun invoke(context: Context, input: Any) {
        val metric = input as PrometheusMetric

        if (metricState.get()?.equals(metric.value) == true) {
            return
        }

        if (metric.value == Double.NaN) {
            return
        }

        metricState.set(metric.value)

        when (metric.name) {
            "kafka_user_fetch_byte_rate",
            "kafka_user_produce_byte_rate" -> context.send(
                KafkaUserFun.TYPE, splitId(context.self()), metric
            )
            "kafka_topic_messages_in_rate",
            "kafka_topic_log_size",
            "kafka_topic_bytes_in_rate",
            "kafka_topic_bytes_out_rate" -> context.send(
                KafkaTopicFun.TYPE, splitId(context.self()), metric
            )
        }
    }
}
