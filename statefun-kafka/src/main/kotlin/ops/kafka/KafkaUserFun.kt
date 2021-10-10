package ops.kafka

import ops.kafka.client.KafkaClient
import ops.protocols.kafka.generated.KafkaCredential
import ops.protocols.kafka.generated.KafkaUser
import ops.protocols.kafka.generated.KafkaUserEvent
import ops.protocols.kafka.generated.KafkaUserRequest
import ops.protocols.kafka.generated.KafkaUserSnapshot
import ops.protocols.kafka.generated.KafkaUserSnapshot.LifecycleStatus
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
import java.time.Instant
import java.util.concurrent.CompletableFuture

class KafkaUserFun(private val options: Options) : StatefulMatchFunction() {
    companion object {
        val TYPE = FunctionType(ModuleIO.FUNCTION_NAMESPACE, "kafka-user")
        val log: Logger = LoggerFactory.getLogger(KafkaUserFun::class.java)
    }

    override fun configure(binder: MatchBinder) {
        binder
            .predicate(KafkaUserRequest::class.java, this::handleRequest)
            .predicate(KafkaUserEvent::class.java, this::handleEvent)
            .predicate(PrometheusMetric::class.java, this::handleMetric)
    }

    @Persisted
    private val userState = PersistedValue.of(
        "user", KafkaUser::class.java,
    )

    @Persisted
    private val credentialsState = PersistedTable.of(
        "credentials", String::class.java, KafkaCredential::class.java,
    )

    @Persisted
    private val metricsState = PersistedTable.of(
        "metrics", String::class.java, Double::class.java,
    )

    private fun handleRequest(context: Context, request: KafkaUserRequest) {
        when (request.typeCase) {
            KafkaUserRequest.TypeCase.ADD_REQUESTED -> {
                if (userState.get() == null) {
                    log.info("Adding user ${request.userName}")
                } else {
                    log.warn("Cannot add user ${request.userName} that already exists")
                }

                val user = KafkaUser.newBuilder()
                    .setUserName(request.userName)
                    .setCreatedTime(System.currentTimeMillis())
                    .putAllQuotas(request.addRequested.quotasMap)
                    .build()

                val event = addUser(user).get()
                userState.set(user)

                context.send(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID, event)
                context.send(ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID, snapshot(user))
            }
            KafkaUserRequest.TypeCase.REMOVE_REQUESTED -> {
                val user = userState.get() ?: run {
                    log.warn("Cannot remove missing user ${request.userName}")
                    return
                }

                log.info("Removing user ${request.userName}")

                val event = removeUser(user).get()
                userState.clear()
                credentialsState.clear()
                metricsState.clear()

                context.send(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID, event)
                context.send(
                    ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID,
                    snapshot(user, lifecycleStatus = LifecycleStatus.GONE)
                )
            }
            KafkaUserRequest.TypeCase.ADD_CREDENTIAL_REQUESTED -> {
                val user = userState.get() ?: run {
                    log.warn("Cannot add credential for missing user ${request.userName}")
                    return
                }

                val identifier = request.addCredentialRequested.identifier
                log.info("Adding user credential $identifier")

                val secretText = request.addCredentialRequested.secretValue
                val credential = KafkaCredential.newBuilder()
                    .setIdentifier(identifier)
                    .setSecretHash(secretText.sha256())
                    .setCreatedTime(System.currentTimeMillis())
                    .setExpirationTime(request.addCredentialRequested.expirationTime)
                    .build()

                val event = addCredential(user, credential, secretText).get()
                credentialsState.set(credential.identifier, credential)

                context.send(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID, event)
                context.send(
                    ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID,
                    snapshot(
                        user,
                        credentialsState.values().toList(),
                        metricsState.asMap(),
                    )
                )

                context.sendAfter(
                    Duration.between(
                        Instant.ofEpochMilli(credential.createdTime),
                        Instant.ofEpochMilli(credential.expirationTime)
                    ),
                    context.self(),
                    KafkaUserEvent.newBuilder()
                        .setUserName(context.self().id())
                        .setEventTime(credential.expirationTime)
                        .setCredentialExpired(
                            KafkaUserEvent.CredentialExpired.newBuilder()
                                .setIdentifier(credential.identifier)
                        )
                        .build(),
                    expirationCancellationToken(identifier),
                )
            }
            KafkaUserRequest.TypeCase.REVOKE_CREDENTIAL_REQUESTED -> {
                val user = userState.get() ?: run {
                    log.warn("Cannot revoke credential for missing user ${request.userName}")
                    return
                }

                val identifier = request.revokeCredentialRequested.identifier
                credentialsState.get(identifier) ?: run {
                    log.warn("Cannot revoke missing credential $identifier")
                    return
                }

                log.info("Revoking user credential $identifier")

                val event = removeCredential(user, identifier).get()
                credentialsState.remove(identifier)

                context.send(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID, event)
                context.send(
                    ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID,
                    snapshot(
                        user,
                        credentialsState.values().toList(),
                        metricsState.asMap(),
                    )
                )
                context.cancelDelayedMessage(
                    expirationCancellationToken(identifier)
                )
            }

            else -> log.warn("Unexpected user request type: ${request.typeCase}")
        }
    }

    private fun handleEvent(context: Context, event: KafkaUserEvent) {
        when (event.typeCase) {
            KafkaUserEvent.TypeCase.CREDENTIAL_EXPIRED -> {
                val user = userState.get() ?: run {
                    log.warn("Unable to expire credential for missing user ${event.userName}")
                    return
                }

                val identifier = event.credentialExpired.identifier
                credentialsState.get(identifier) ?: run {
                    log.warn("Unable to expire missing credential $identifier")
                    return
                }

                log.info("Expiring user credential $identifier")

                removeCredential(user, identifier).get()
                credentialsState.remove(identifier)

                context.send(ModuleIO.KAFKA_USER_EVENT_EGRESS_ID, event)
                context.send(
                    ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID,
                    snapshot(
                        user,
                        credentialsState.values().toList(),
                        metricsState.asMap(),
                    )
                )
            }
            else -> log.warn("Unexpected user event type: ${event.typeCase}")
        }
    }

    private fun handleMetric(context: Context, metric: PrometheusMetric) {
        val user = userState.get() ?: run {
            log.warn("Cannot set metrics for missing user ${context.self().id()}")
            return
        }

        metricsState.set(metric.name, metric.value)

        context.send(
            ModuleIO.KAFKA_USER_SNAPSHOT_EGRESS_ID,
            snapshot(user, credentialsState.values().toList(), metricsState.asMap())
        )
    }

    private fun addUser(user: KafkaUser): CompletableFuture<KafkaUserEvent> =
        CompletableFuture.supplyAsync {
            KafkaClient(options.asKafkaProperties()).use { client ->
                client.applyUserQuotas(user.userName, user.quotasMap)
            }
        }.whenComplete { _, throwable ->
            if (throwable != null) log.error("Failed to add a user: $throwable")
        }.thenApply {
            KafkaUserEvent.newBuilder()
                .setUserName(user.userName)
                .setEventTime(System.currentTimeMillis())
                .setUserAdded(
                    KafkaUserEvent.UserAdded.newBuilder()
                        .putAllQuotas(user.quotasMap)
                )
                .build()
        }

    private fun removeUser(user: KafkaUser): CompletableFuture<KafkaUserEvent> =
        CompletableFuture.supplyAsync {
            KafkaClient(options.asKafkaProperties()).use { client ->
                client.removeUserQuotas(user.userName, user.quotasMap.keys.toList())
            }
        }.whenComplete { _, throwable ->
            if (throwable != null) log.error("Failed to remove a user: $throwable")
        }.thenApply {
            KafkaUserEvent.newBuilder()
                .setUserName(user.userName)
                .setEventTime(System.currentTimeMillis())
                .setUserRemoved(
                    KafkaUserEvent.UserRemoved.newBuilder()
                )
                .build()
        }

    private fun addCredential(
        user: KafkaUser,
        credential: KafkaCredential,
        secretText: String,
    ): CompletableFuture<KafkaUserEvent> =
        CompletableFuture.supplyAsync {
            KafkaClient(options.asKafkaProperties()).use { client ->
                client.addCredential(credential.identifier, secretText)
            }
        }.whenComplete { _, throwable ->
            if (throwable != null) log.error("Failed to add a credential: $throwable")
        }.thenApply {
            KafkaUserEvent.newBuilder()
                .setUserName(user.userName)
                .setEventTime(System.currentTimeMillis())
                .setCredentialAdded(
                    KafkaUserEvent.CredentialAdded.newBuilder()
                        .setIdentifier(credential.identifier)
                        .setSecretHash(credential.secretHash)
                        .setExpirationTime(credential.expirationTime)
                )
                .build()
        }

    private fun removeCredential(
        user: KafkaUser,
        identifier: String,
    ): CompletableFuture<KafkaUserEvent> =
        CompletableFuture.supplyAsync {
            KafkaClient(options.asKafkaProperties()).use { client ->
                client.removeCredential(identifier)
            }
        }.whenComplete { _, throwable ->
            if (throwable != null) log.error("Failed to add a user: $throwable")
        }.thenApply {
            KafkaUserEvent.newBuilder()
                .setUserName(user.userName)
                .setEventTime(System.currentTimeMillis())
                .setCredentialExpired(
                    KafkaUserEvent.CredentialExpired.newBuilder()
                        .setIdentifier(identifier)
                )
                .build()
        }

    private fun expirationCancellationToken(identifier: String): String =
        "expire-credential-$identifier"

    private fun snapshot(
        user: KafkaUser,
        credentials: List<KafkaCredential> = emptyList(),
        metrics: Map<String, Double> = emptyMap(),
        lifecycleStatus: LifecycleStatus = LifecycleStatus.OK
    ): KafkaUserSnapshot =
        KafkaUserSnapshot.newBuilder()
            .setUserName(user.userName)
            .setCreatedTime(user.createdTime)
            .setLifecycleStatus(lifecycleStatus)
            .putAllQuotas(user.quotasMap)
            .putAllRecentMetrics(metrics)
            .addAllCredentials(credentials)
            .build()
}
