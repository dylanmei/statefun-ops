package ops.kafka.client

import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import java.io.Closeable
import java.util.Properties
import java.util.concurrent.CompletableFuture

class KafkaClient(properties: Properties) : Closeable {
    companion object {
        const val timeoutMs = 10000
    }

    private val client: AdminClient = AdminClient.create(properties)

    fun addTopic(
        topicName: String,
        partitionCount: Int,
        replicationFactor: Int,
        topicConfig: Map<String, String>,
    ): CompletableFuture<Void> {
        val options = CreateTopicsOptions().timeoutMs(timeoutMs)
        val result = client.createTopics(
            listOf(
                NewTopic(
                    topicName,
                    partitionCount,
                    replicationFactor.toShort(),
                ).configs(topicConfig)
            ),
            options
        )

        return result.all().asCompletableFuture<Void>()
    }

    fun removeTopic(
        topicName: String,
    ): CompletableFuture<Void> {
        val options = DeleteTopicsOptions().timeoutMs(timeoutMs)
        val result = client.deleteTopics(listOf(topicName), options)

        return result.all().asCompletableFuture<Void>()
    }

    fun applyUserQuotas(
        userName: String,
        quotasMap: Map<String, Long>,
    ): CompletableFuture<Void> {
        val options = AlterClientQuotasOptions().timeoutMs(timeoutMs)
        val operations = quotasMap.map {
            ClientQuotaAlteration.Op(it.key, it.value.toDouble())
        }

        val quotaEntry = ClientQuotaAlteration(
            ClientQuotaEntity(mapOf("user" to userName)),
            operations,
        )

        val result = client.alterClientQuotas(listOf(quotaEntry), options)
        return result.all().asCompletableFuture<Void>()
    }

    fun removeUserQuotas(
        userName: String,
        quotaNames: List<String>,
    ): CompletableFuture<Void> {
        val options = AlterClientQuotasOptions().timeoutMs(timeoutMs)
        val operations = quotaNames.map {
            ClientQuotaAlteration.Op(it, null)
        }

        val quotaEntry = ClientQuotaAlteration(
            ClientQuotaEntity(mapOf("user" to userName)),
            operations,
        )

        val result = client.alterClientQuotas(listOf(quotaEntry), options)
        return result.all().asCompletableFuture<Void>()
    }

    fun addCredential(
        identifier: String,
        secretText: String,
    ): CompletableFuture<Void> {
        val options = AlterUserScramCredentialsOptions().timeoutMs(timeoutMs)
        val upserts = listOf(
            UserScramCredentialUpsertion(
                identifier,
                ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096),
                secretText.toByteArray()
            ),
            UserScramCredentialUpsertion(
                identifier,
                ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096),
                secretText.toByteArray()
            ),
        )
        val result = client.alterUserScramCredentials(upserts, options)
        return result.all().asCompletableFuture<Void>()
    }

    fun removeCredential(
        identifier: String,
    ): CompletableFuture<Void> {
        val options = AlterUserScramCredentialsOptions().timeoutMs(timeoutMs)
        val deletes = listOf(
            UserScramCredentialDeletion(
                identifier,
                ScramMechanism.SCRAM_SHA_256,
            ),
            UserScramCredentialDeletion(
                identifier,
                ScramMechanism.SCRAM_SHA_512,
            ),
        )
        val result = client.alterUserScramCredentials(deletes, options)
        return result.all().asCompletableFuture<Void>()
    }

    override fun close() {
        client.close()
    }
}
