package ops.kafka.client

import org.apache.kafka.common.KafkaFuture
import java.util.concurrent.CompletableFuture

fun <T> KafkaFuture<T>.asCompletableFuture(): CompletableFuture<T> {
    val wrappingFuture = CompletableFuture<T>()
    this.whenComplete { value, throwable ->
        if (throwable != null) {
            wrappingFuture.completeExceptionally(throwable)
        } else {
            wrappingFuture.complete(value)
        }
    }

    return wrappingFuture
}
