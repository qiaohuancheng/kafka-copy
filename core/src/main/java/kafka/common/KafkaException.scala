package kafka.common

/**
 * @author zhaori
 */
class KafkaException(message: String, t:Throwable) extends RuntimeException(message, t) {
    def this(message: String) = this(message, null)
    def this(t: Throwable) = this("",t)
}