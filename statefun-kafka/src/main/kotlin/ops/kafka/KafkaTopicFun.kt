package ops.kafka

import ops.kafka.client.KafkaClient
import ops.protocols.kafka.generated.KafkaTopic
import ops.protocols.kafka.generated.KafkaTopicEvent
import ops.protocols.kafka.generated.KafkaTopicMessage
import ops.protocols.kafka.generated.KafkaTopicRequest
import ops.protocols.kafka.generated.KafkaTopicSnapshot
import ops.protocols.kafka.generated.KafkaTopicSnapshot.LifecycleStatus
import ops.protocols.prometheus.generated.PrometheusMetric
import org.apache.flink.statefun.sdk.Context
import org.apache.flink.statefun.sdk.FunctionType
import org.apache.flink.statefun.sdk.annotations.Persisted
import org.apache.flink.statefun.sdk.match.MatchBinder
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction
import org.apache.flink.statefun.sdk.state.PersistedTable
import org.apache.flink.statefun.sdk.state.PersistedValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

class KafkaTopicFun(private val options: Options) : StatefulMatchFunction() {
    companion object {
        val TYPE = FunctionType(ModuleIO.FUNCTION_NAMESPACE, "kafka-topic")
        val log: Logger = LoggerFactory.getLogger(KafkaTopicFun::class.java)
        const val CANCELLATION_TOKEN_CHECK_DELETE_POLICY = "check-delete-policy"
    }

    @Persisted
    private val topicState = PersistedValue.of(
        "topic", KafkaTopic::class.java,
    )

    @Persisted
    private val metricsState = PersistedTable.of(
        "metrics", String::class.java, Double::class.java,
    )

    override fun configure(binder: MatchBinder) {
        binder
            .predicate(KafkaTopicRequest::class.java, this::handleRequest)
            .predicate(KafkaTopicMessage::class.java, this::handleMessage)
            .predicate(PrometheusMetric::class.java, this::handleMetric)
    }

    private fun handleRequest(context: Context, request: KafkaTopicRequest) {
        when (request.typeCase) {
            KafkaTopicRequest.TypeCase.ADD_REQUESTED -> {
                if (topicState.get() == null) {
                    log.info("Adding topic ${request.topicName}")
                } else {
                    log.warn("Skipping topic ${request.topicName} that already exists")
                    return
                }

                val topic = KafkaTopic.newBuilder()
                    .setTopicName(request.topicName)
                    .setPartitionCount(request.addRequested.partitionCount)
                    .setReplicationFactor(request.addRequested.replicationFactor)
                    .putAllTopicConfig(request.addRequested.topicConfigMap)
                    .setDeletePolicy(request.addRequested.deletePolicy)
                    .build()

                val event = addTopic(topic).get()
                topicState.set(topic)

                if (topic.hasDeletePolicy() && topic.deletePolicy.waitTime > 0) {
                    context.sendAfter(
                        Duration.ofMillis(topic.deletePolicy.waitTime),
                        context.self(),
                        KafkaTopicMessage.newBuilder()
                            .setCheckDeletePolicy(KafkaTopicMessage.CheckDeletePolicy.newBuilder())
                            .build(),
                        CANCELLATION_TOKEN_CHECK_DELETE_POLICY,
                    )
                }

                context.send(ModuleIO.KAFKA_TOPIC_EVENT_EGRESS_ID, event)
                context.send(
                    ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID,
                    snapshot(topic),
                )
            }
            KafkaTopicRequest.TypeCase.REMOVE_REQUESTED -> {
                log.info("Removing topic ${request.topicName}")

                val topic = topicState.get() ?: run {
                    log.warn("Unable to remove missing topic ${request.topicName}")
                    return
                }

                val event = removeTopic(topic.topicName).get()
                topicState.clear()

                context.send(ModuleIO.KAFKA_TOPIC_EVENT_EGRESS_ID, event)
                context.send(
                    ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID,
                    snapshot(topic, lifecycleStatus = LifecycleStatus.GONE)
                )
            }
            else -> log.warn("Unexpected Kafka topic request type: ${request.typeCase}")
        }
    }

    private fun handleMessage(context: Context, message: KafkaTopicMessage) {
        when (message.typeCase) {
            KafkaTopicMessage.TypeCase.CHECK_DELETE_POLICY -> {
                log.info("Checking if topic ${context.self().id()} can be deleted")

                val topic = topicState.get() ?: run {
                    log.warn("Unable to check missing topic ${context.self().id()}")
                    return
                }

                val canDelete = when {
                    topic.deletePolicy.hasLogSizePolicy() -> {
                        val logSize = metricsState.get("kafka_topic_log_size") ?: 0.0
                        logSize <= topic.deletePolicy.logSizePolicy.lteSize
                    }
                    topic.deletePolicy.hasMessagesInRatePolicy() -> {
                        val messagesInRate = metricsState.get("kafka_topic_messages_in_rate") ?: 0.0
                        messagesInRate <= topic.deletePolicy.messagesInRatePolicy.lteRate
                    }
                    else -> {
                        log.warn("Skipping topic ${context.self().id()} with no delete policy")
                        false
                    }
                }

                if (canDelete) {
                    log.info("Removing topic ${topic.topicName}")

                    val event = removeTopic(topic.topicName).get()
                    topicState.clear()

                    context.send(ModuleIO.KAFKA_TOPIC_EVENT_EGRESS_ID, event)
                    context.send(
                        ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID,
                        snapshot(topic, lifecycleStatus = LifecycleStatus.GONE)
                    )
                }
            }
            else -> log.warn("Unexpected topic message type: ${message.typeCase}")
        }
    }

    private fun handleMetric(context: Context, metric: PrometheusMetric) {
        val topic = topicState.get() ?: run {
            log.warn("Cannot set metrics for missing topic ${context.self().id()}")
            return
        }

        metricsState.set(metric.name, metric.value)

        context.send(
            ModuleIO.KAFKA_TOPIC_SNAPSHOT_EGRESS_ID,
            snapshot(topic, metricsState.asMap())
        )

        // Invalidate a delete policy
        if (topic.hasDeletePolicy()) {
            val canScheduleMessage = when (metric.name) {
                "kafka_topic_log_size" -> topic.deletePolicy.hasLogSizePolicy()
                "kafka_topic_messages_in_rate" -> topic.deletePolicy.hasMessagesInRatePolicy()
                else -> false
            }

            if (canScheduleMessage) {
                context.cancelDelayedMessage(CANCELLATION_TOKEN_CHECK_DELETE_POLICY)
                context.sendAfter(
                    Duration.ofMillis(topic.deletePolicy.waitTime),
                    context.self(),
                    KafkaTopicMessage.newBuilder()
                        .setCheckDeletePolicy(KafkaTopicMessage.CheckDeletePolicy.newBuilder())
                        .build(),
                    CANCELLATION_TOKEN_CHECK_DELETE_POLICY,
                )
            }
        }
    }

    private fun addTopic(topic: KafkaTopic): CompletableFuture<KafkaTopicEvent> =
        CompletableFuture.supplyAsync {
            KafkaClient(options.asKafkaProperties()).use { client ->
                client.addTopic(
                    topic.topicName,
                    topic.partitionCount,
                    topic.replicationFactor,
                    topic.topicConfigMap,
                )
            }
        }.whenComplete { _, throwable ->
            if (throwable != null) log.error("Unable to add a topic: $throwable")
        }.thenApply {
            KafkaTopicEvent.newBuilder()
                .setTopicName(topic.topicName)
                .setEventTime(System.currentTimeMillis())
                .setTopicAdded(
                    KafkaTopicEvent.TopicAdded.newBuilder()
                        .setPartitionCount(topic.partitionCount)
                        .setReplicationFactor(topic.replicationFactor)
                        .putAllTopicConfig(topic.topicConfigMap)
                        .setDeletePolicy(topic.deletePolicy)
                )
                .build()
        }

    private fun removeTopic(topicName: String): CompletableFuture<KafkaTopicEvent> =
        CompletableFuture.supplyAsync {
            KafkaClient(options.asKafkaProperties()).use { client ->
                client.removeTopic(topicName)
            }
        }.whenComplete { _, throwable ->
            if (throwable != null) log.error("Unable to remove a topic: $throwable")
        }.thenApply {
            KafkaTopicEvent.newBuilder()
                .setTopicName(topicName)
                .setEventTime(System.currentTimeMillis())
                .setTopicRemoved(KafkaTopicEvent.TopicRemoved.newBuilder())
                .build()
        }

    private fun snapshot(
        topic: KafkaTopic,
        metrics: Map<String, Double> = emptyMap(),
        lifecycleStatus: LifecycleStatus = LifecycleStatus.OK
    ): KafkaTopicSnapshot =
        KafkaTopicSnapshot.newBuilder()
            .setTopicName(topic.topicName)
            .setCreatedTime(topic.createdTime)
            .setPartitionCount((topic.partitionCount))
            .setReplicationFactor(topic.replicationFactor)
            .putAllTopicConfig(topic.topicConfigMap)
            .setDeletePolicy(topic.deletePolicy)
            .setLifecycleStatus(lifecycleStatus)
            .putAllRecentMetrics(metrics)
            .build()
}
