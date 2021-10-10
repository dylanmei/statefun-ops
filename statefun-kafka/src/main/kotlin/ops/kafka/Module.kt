package ops.kafka

import com.google.auto.service.AutoService
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@AutoService(StatefulFunctionModule::class)
class Module : StatefulFunctionModule {
    companion object {
        val log: Logger = LoggerFactory.getLogger(Module::class.java)
    }

    override fun configure(globalConfiguration: Map<String, String>, binder: StatefulFunctionModule.Binder) {
        log.debug("Configuring our Kafka module")

        ModuleIO(globalConfiguration).apply {
            // Bind an ingress and router for Kafka topic requests
            binder.bindIngress(topicRequestIngressSpec)
            binder.bindIngressRouter(ModuleIO.KAFKA_TOPIC_REQUEST_INGRESS_ID, KafkaTopicRequestRouter())

            // Bind an ingress and router for Kafka user requests
            binder.bindIngress(userRequestIngressSpec)
            binder.bindIngressRouter(ModuleIO.KAFKA_USER_REQUEST_INGRESS_ID, KafkaUserRequestRouter())

            // Bind an ingress for Prometheus metrics
            binder.bindIngress(metricIngressSpec)
            binder.bindIngressRouter(ModuleIO.PROMETHEUS_METRIC_INGRESS_ID, PrometheusMetricRouter())

            // Bind an egress to emit Kafka topic snapshots
            binder.bindEgress(topicSnapshotEgressSpec)

            // Bind an egress to emit Kafka topic events
            binder.bindEgress(topicEventEgressSpec)

            // Bind an egress to emit Kafka topic snapshots
            binder.bindEgress(userSnapshotEgressSpec)

            // Bind an egress to emit Kafka topic events
            binder.bindEgress(userEventEgressSpec)

            // Bind a function provider to a function type
            binder.bindFunctionProvider(KafkaTopicFun.TYPE) {
                topicFun
            }
            binder.bindFunctionProvider(KafkaUserFun.TYPE) {
                userFun
            }
            binder.bindFunctionProvider(MetricFun.TYPE) {
                metricFun
            }
        }
    }
}
