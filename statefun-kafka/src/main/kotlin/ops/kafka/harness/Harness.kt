package ops.kafka.harness

import ops.kafka.ModuleIO
import ops.kafka.Options
import ops.protocols.kafka.generated.KafkaTopicSnapshot
import ops.protocols.kafka.generated.KafkaTopicEvent
import ops.protocols.kafka.generated.KafkaUserEvent
import ops.protocols.kafka.generated.KafkaUserSnapshot
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.statefun.flink.harness.Harness as StatefunHarness

object Harness {
    val log: Logger = LoggerFactory.getLogger(Module::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("Starting development harness")
        if (args.contains("--generator")) {
            generatorHarness().start()
        } else {
            dockerComposeHarness(
                Options.fromArgs(args)
            ).start()
        }
    }

    fun generatorHarness() = StatefunHarness()
        .withFlinkJobName("statefun-kafka")
        .withConfiguration("parallelism.default", "1")
        .withSupplyingIngress(ModuleIO.KAFKA_TOPIC_REQUEST_INGRESS_ID, KafkaTopicRequestGenerator(20000L))
        //.withSupplyingIngress(ModuleIO.PROMETHEUS_METRIC_INGRESS_ID, PrometheusMetricGenerator(10000L))
        .withConsumingEgress(
            ModuleIO.KAFKA_TOPIC_EVENT_EGRESS_ID,
            StdoutPrintingConsumer<KafkaTopicEvent>("EVENT>")
        )
        .withConsumingEgress(
            ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID,
            StdoutPrintingConsumer<KafkaTopicSnapshot>("SNAPSHOT>")
        )

    fun dockerComposeHarness(options: Options): StatefunHarness {
        val harness = StatefunHarness()
            .withFlinkJobName("statefun-kafka")
            .withConfiguration("parallelism.default", "1")
            .withFlinkSourceFunction(
                ModuleIO.KAFKA_TOPIC_REQUEST_INGRESS_ID,
                KafkaTopicRequestSource(options)
            )
            //.withFlinkSourceFunction(
            //    ModuleIO.PROMETHEUS_METRIC_INGRESS_ID,
            //    PrometheusMetricSource(options)
            //)
            .withConsumingEgress(
                ModuleIO.KAFKA_TOPIC_EVENT_EGRESS_ID,
                StdoutPrintingConsumer<KafkaTopicEvent>("TOPIC EVENT>")
            )
            .withConsumingEgress(
                ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID,
                StdoutPrintingConsumer<KafkaTopicSnapshot>("TOPIC SNAPSHOT>")
            )
            .withConsumingEgress(
                ModuleIO.KAFKA_USER_EVENT_EGRESS_ID,
                StdoutPrintingConsumer<KafkaUserEvent>("USER EVENT>")
            )
            .withConsumingEgress(
                ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID,
                StdoutPrintingConsumer<KafkaUserSnapshot>("USER SNAPSHOT>")
            )

        options.forEach { (key, value) ->
            harness.withGlobalConfiguration(key, value)
        }

        return harness
    }
}
