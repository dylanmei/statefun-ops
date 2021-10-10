package ops.kafka.harness

import ops.protocols.prometheus.generated.PrometheusLabel
import ops.protocols.prometheus.generated.PrometheusMetric
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier

class PrometheusMetricGenerator(val intervalMs: Long) : SerializableSupplier<PrometheusMetric> {
    val topicName = "hello-stream"
    var requests = mutableListOf<PrometheusMetric>()

    override fun get(): PrometheusMetric {
        if (requests.isEmpty()) {
            resetRequests()
            Thread.sleep(intervalMs)
        }

        return requests.removeAt(0)
    }

    fun resetRequests() {
        requests = mutableListOf(
            PrometheusMetric.newBuilder().setTimestamp(System.currentTimeMillis())
                .setName("kafka_topic_messages_in_rate")
                .setValue(1.23456)
                .addLabels(
                    PrometheusLabel.newBuilder()
                        .setName("topic")
                        .setValue(topicName)
                ).build(),
            PrometheusMetric.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setName("kafka_topic_log_size")
                .setValue(123.456)
                .addLabels(
                    PrometheusLabel.newBuilder()
                        .setName("topic")
                        .setValue(topicName)
                ).build(),
        )
    }
}
