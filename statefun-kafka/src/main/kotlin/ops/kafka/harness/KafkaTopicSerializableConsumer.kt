package ops.kafka.harness

//import ops.kafka.KafkaTopicSerializationSchema
//import ops.kafka.Options
//import ops.kafka.model.KafkaTopic
//import org.apache.flink.statefun.flink.harness.io.SerializableConsumer
//import org.apache.kafka.clients.producer.KafkaProducer
//
//class KafkaTopicSerializableConsumer(
//    val options: Options,
//    val serializationSchema: KafkaTopicSerializationSchema,
//) : SerializableConsumer<KafkaTopic> {
//
//    var ready: Boolean = false
//    lateinit var kafkaProducer: KafkaProducer<ByteArray, ByteArray>
//
//    fun setup() {
//        ready = true
//        val kafkaProperties = options.asKafkaProperties()
//        kafkaProperties["key.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
//        kafkaProperties["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
//        kafkaProperties["batch.size"] = "0"
//        kafkaProducer = KafkaProducer<ByteArray, ByteArray>(kafkaProperties)
//    }
//
//    override fun accept(kafkaTopic: KafkaTopic) {
//        if (!ready) setup()
//        kafkaProducer.send(serializationSchema.serialize(kafkaTopic, null))
//    }
//}
//
