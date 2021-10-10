package ops.kafka.harness

import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.util.JsonFormat
import org.apache.flink.statefun.flink.harness.io.SerializableConsumer

class StdoutPrintingConsumer<T>(val prefix: String? = null) : SerializableConsumer<T> {
    override fun accept(obj: T) {
        val message = when (obj) {
            is MessageOrBuilder -> printer.print(obj)
            else -> obj.toString()
        }
        prefix?.run {
            print("$prefix ")
        }

        println(message)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        val printer: JsonFormat.Printer = JsonFormat.printer()
    }
}
