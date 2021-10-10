package ops.kafka

import ops.protocols.kafka.generated.KafkaTopicRequest
import ops.protocols.kafka.generated.KafkaUserRequest
import ops.protocols.prometheus.generated.PrometheusMetric

import org.apache.flink.statefun.sdk.io.Router
import org.apache.flink.statefun.sdk.io.Router.Downstream

class KafkaTopicRequestRouter : Router<KafkaTopicRequest> {
    override fun route(request: KafkaTopicRequest, downstream: Downstream<KafkaTopicRequest>) {
        downstream.forward(KafkaTopicFun.TYPE, request.topicName, request)
    }
}

class KafkaUserRequestRouter : Router<KafkaUserRequest> {
    override fun route(request: KafkaUserRequest, downstream: Downstream<KafkaUserRequest>) {
        downstream.forward(KafkaUserFun.TYPE, request.userName, request)
    }
}

class PrometheusMetricRouter : Router<PrometheusMetric> {
    override fun route(metric: PrometheusMetric, downstream: Downstream<PrometheusMetric>) {
        val contextId = when (metric.name) {
            "kafka_topic_messages_in_rate" -> metric.labelsList.find { it.name == "topic" }?.value
            "kafka_topic_log_size" -> metric.labelsList.find { it.name == "topic" }?.value
            "kafka_topic_bytes_in_rate" -> metric.labelsList.find { it.name == "topic" }?.value
            "kafka_topic_bytes_out_rate" -> metric.labelsList.find { it.name == "topic" }?.value
            "kafka_quota_fetch_byte_rate" -> metric.labelsList.find { it.name == "user" }?.value
            "kafka_quota_produce_byte_rate" -> metric.labelsList.find { it.name == "user" }?.value
            else -> null
        }

        contextId?.let {
            downstream.forward(MetricFun.TYPE, "${metric.name}:$contextId", metric)
        }
    }
}
