package kafka.common

/**
 * @author zhaori
 */
class UnknownCodecException(message: String) extends RuntimeException(message) {
    def this() = this(null)
}