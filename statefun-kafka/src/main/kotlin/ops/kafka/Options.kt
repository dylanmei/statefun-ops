package ops.kafka

import org.apache.flink.api.java.utils.ParameterTool
import java.io.Serializable

class Options(val map: Map<String, String>) :
    Serializable, Cloneable {
    companion object {
        fun fromArgs(args: Array<String>) =
            Options(ParameterTool.fromArgs(args).toMap())

        const val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
        const val KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.group.id"
        const val KAFKA_SASL_USERNAME = "kafka.sasl.username"
        const val KAFKA_SASL_PASSWORD = "kafka.sasl.password"
        const val DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
        const val DEFAULT_CONSUMER_GROUP_ID = "statefun-ops"
        const val DEFAULT_SASL_USERNAME = "statefun"
        const val DEFAULT_SASL_PASSWORD = "statefun"

        private const val serialVersionUID: Long = 1L
    }

    val kafkaAddress
        get() = map.getOrDefault(KAFKA_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS)

    fun asKafkaProperties() = mapOf(
        "bootstrap.servers" to kafkaAddress,
        "security.protocol" to "SASL_PLAINTEXT",
        "sasl.mechanism" to "PLAIN",
        "sasl.jaas.config" to "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"${map.getOrDefault(KAFKA_SASL_USERNAME, DEFAULT_SASL_USERNAME)}\" " +
            "password=\"${map.getOrDefault(KAFKA_SASL_PASSWORD, DEFAULT_SASL_PASSWORD)}\";",

        "linger.ms" to "1000",
        "transaction.timeout.ms" to "600000",

        "group.id" to map.getOrDefault(KAFKA_CONSUMER_GROUP_ID, DEFAULT_CONSUMER_GROUP_ID),
        "max.poll.records" to "50",

    ).toProperties()

    fun forEach(action: (Map.Entry<String, String>) -> Unit) {
        for (element in map) action(element)
    }
}
