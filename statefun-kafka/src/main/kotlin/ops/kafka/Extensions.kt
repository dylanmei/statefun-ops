package ops.kafka

import org.apache.flink.statefun.sdk.state.PersistedTable
import java.math.BigInteger
import java.security.MessageDigest

fun String.sha256(): String {
    val md = MessageDigest.getInstance("SHA-256")
    return BigInteger(1, md.digest(toByteArray())).toString(16).padStart(32, '0')
}

fun <K, V> PersistedTable<K, V>.asMap(): Map<K, V> {
    return this.entries().associate {
        it.key to it.value
    }
}
