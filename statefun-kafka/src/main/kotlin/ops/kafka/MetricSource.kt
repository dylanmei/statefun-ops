package ops.kafka

import ops.kafka.serde.PrometheusMetricDeserializationSchema
import ops.protocols.prometheus.generated.PrometheusMetric
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class MetricSource(
    options: Options
) : FlinkKafkaConsumer<PrometheusMetric>(
    "prometheus-metrics",
    PrometheusMetricDeserializationSchema(),
    options.asKafkaProperties(),
) {
    override fun open(configuration: Configuration) {
        setStartFromLatest()
        setCommitOffsetsOnCheckpoints(false)
        super.open(configuration)
    }
}
