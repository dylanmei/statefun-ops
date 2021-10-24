package ops.kafka.harness

import ops.protocols.kafka.generated.KafkaTopicRequest
import ops.protocols.kafka.generated.KafkaUserRequest
import java.time.Instant
import java.time.temporal.ChronoUnit

object Generator {
    private val defaultQuotas: Map<String, Long> = mapOf(
        "consumer_byte_rate" to 1024 * 1024L,
        "producer_byte_rate" to 1024 * 1024L,
    )

    fun addTopicRequested(
        topicName: String,
        partitionCount: Int = 1,
        replicationFactor: Int = 2,
    ): KafkaTopicRequest =
        KafkaTopicRequest.newBuilder()
            .setTopicName(topicName)
            .setAddRequested(
                KafkaTopicRequest.AddRequested.newBuilder()
                    .setPartitionCount(partitionCount)
                    .setReplicationFactor(replicationFactor)
                    .build()
            )
            .build()

    fun removeTopicRequested(topicName: String): KafkaTopicRequest =
        KafkaTopicRequest.newBuilder()
            .setTopicName(topicName)
            .setRemoveRequested(
                KafkaTopicRequest.RemoveRequested.newBuilder()
                    .build()
            )
            .build()

    fun addUserRequested(
        userName: String,
        quotas: Map<String, Long> = defaultQuotas,
    ): KafkaUserRequest =
        KafkaUserRequest.newBuilder()
            .setUserName(userName)
            .setAddRequested(
                KafkaUserRequest.AddRequested.newBuilder()
                    .putAllQuotas(quotas)
                    .build()
            )
            .build()

    fun removeUserRequested(userName: String): KafkaUserRequest =
        KafkaUserRequest.newBuilder()
            .setUserName(userName)
            .setRemoveRequested(
                KafkaUserRequest.RemoveRequested.newBuilder()
                    .build()
            )
            .build()

    fun addCredentialRequested(
        userName: String,
        identifier: String,
        secret: String = "statefun",
        expiration: Instant = Instant.now().plus(60, ChronoUnit.SECONDS)
    ): KafkaUserRequest = KafkaUserRequest.newBuilder()
        .setUserName(userName)
        .setAddCredentialRequested(
            KafkaUserRequest.AddCredentialRequested.newBuilder()
                .setIdentifier(identifier)
                .setSecretValue(secret)
                .setExpirationTime(expiration.toEpochMilli())
                .build()
        )
        .build()

    fun revokeCredentialRequested(
        userName: String,
        identifier: String,
    ): KafkaUserRequest = KafkaUserRequest.newBuilder()
        .setUserName(userName)
        .setRevokeCredentialRequested(
            KafkaUserRequest.RevokeCredentialRequested.newBuilder()
                .setIdentifier(identifier)
                .build()
        )
        .build()
}
