package ops.kafka

import ops.kafka.serde.*
import ops.protocols.kafka.generated.*
import ops.protocols.prometheus.generated.*
import org.apache.flink.statefun.flink.io.datastream.SourceFunctionSpec
import org.apache.flink.statefun.sdk.io.EgressIdentifier
import org.apache.flink.statefun.sdk.io.EgressSpec
import org.apache.flink.statefun.sdk.io.IngressIdentifier
import org.apache.flink.statefun.sdk.io.IngressSpec
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder

class ModuleIO(config: Map<String, String>) {
    companion object {
        const val FUNCTION_NAMESPACE = "ops.kafka"

        val KAFKA_TOPIC_REQUEST_INGRESS_ID: IngressIdentifier<KafkaTopicRequest> = IngressIdentifier(
            KafkaTopicRequest::class.java, FUNCTION_NAMESPACE, "kafka-topic-requests"
        )

        val KAFKA_USER_REQUEST_INGRESS_ID: IngressIdentifier<KafkaUserRequest> = IngressIdentifier(
            KafkaUserRequest::class.java, FUNCTION_NAMESPACE, "kafka-user-requests"
        )

        val PROMETHEUS_METRIC_INGRESS_ID: IngressIdentifier<PrometheusMetric> = IngressIdentifier(
            PrometheusMetric::class.java, FUNCTION_NAMESPACE, "prometheus-metrics"
        )

        val KAFKA_TOPIC_EVENT_EGRESS_ID: EgressIdentifier<KafkaTopicEvent> = EgressIdentifier(
            FUNCTION_NAMESPACE, "kafka-topic-events", KafkaTopicEvent::class.java
        )

        val KAFKA_TOPIC_SNAPSHOT_EGRESS_ID: EgressIdentifier<KafkaTopicSnapshot> = EgressIdentifier(
            FUNCTION_NAMESPACE, "kafka-topic-snapshots", KafkaTopicSnapshot::class.java
        )

        val KAFKA_USER_EVENT_EGRESS_ID: EgressIdentifier<KafkaUserEvent> = EgressIdentifier(
            FUNCTION_NAMESPACE, "kafka-user-events", KafkaUserEvent::class.java
        )

        val KAFKA_USER_SNAPSHOT_EGRESS_ID: EgressIdentifier<KafkaUserSnapshot> = EgressIdentifier(
            FUNCTION_NAMESPACE, "kafka-user-snapshots", KafkaUserSnapshot::class.java
        )
    }

    val options = Options(config)

    val topicRequestIngressSpec: IngressSpec<KafkaTopicRequest>
        get() = KafkaIngressBuilder.forIdentifier(KAFKA_TOPIC_REQUEST_INGRESS_ID)
            .withKafkaAddress(options.kafkaAddress)
            .withProperties(options.asKafkaProperties())
            .withTopic("kafka-topic-requests")
            .withDeserializer(KafkaTopicRequestDeserializer::class.java)
            .build()

    val metricIngressSpec: IngressSpec<PrometheusMetric>
        get() = SourceFunctionSpec(PROMETHEUS_METRIC_INGRESS_ID, MetricSource(options))

    val userRequestIngressSpec: IngressSpec<KafkaUserRequest>
        get() = KafkaIngressBuilder.forIdentifier(KAFKA_USER_REQUEST_INGRESS_ID)
            .withKafkaAddress(options.kafkaAddress)
            .withProperties(options.asKafkaProperties())
            .withTopic("kafka-user-requests")
            .withDeserializer(KafkaUserRequestDeserializer::class.java)
            .build()

    val topicFun: KafkaTopicFun
        get() = KafkaTopicFun(options)

    val userFun: KafkaUserFun
        get() = KafkaUserFun(options)

    val metricFun: MetricFun
        get() = MetricFun()

    val topicEventEgressSpec: EgressSpec<KafkaTopicEvent>
        get() = KafkaEgressBuilder.forIdentifier(KAFKA_TOPIC_EVENT_EGRESS_ID)
            .withKafkaAddress(options.kafkaAddress)
            .withProperties(options.asKafkaProperties())
            .withSerializer(KafkaTopicEventSerializer::class.java)
            .build()

    val topicSnapshotEgressSpec: EgressSpec<KafkaTopicSnapshot>
        get() = KafkaEgressBuilder.forIdentifier(KAFKA_TOPIC_SNAPSHOT_EGRESS_ID)
            .withKafkaAddress(options.kafkaAddress)
            .withProperties(options.asKafkaProperties())
            .withSerializer(KafkaTopicSnapshotSerializer::class.java)
            .build()

    val userEventEgressSpec: EgressSpec<KafkaUserEvent>
        get() = KafkaEgressBuilder.forIdentifier(KAFKA_USER_EVENT_EGRESS_ID)
            .withKafkaAddress(options.kafkaAddress)
            .withProperties(options.asKafkaProperties())
            .withSerializer(KafkaUserEventSerializer::class.java)
            .build()

    val userSnapshotEgressSpec: EgressSpec<KafkaUserSnapshot>
        get() = KafkaEgressBuilder.forIdentifier(KAFKA_USER_SNAPSHOT_EGRESS_ID)
            .withKafkaAddress(options.kafkaAddress)
            .withProperties(options.asKafkaProperties())
            .withSerializer(KafkaUserSnapshotSerializer::class.java)
            .build()
}
