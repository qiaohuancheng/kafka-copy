package kafka.network

/**
 * @author zhaori
 */
class InvalidRequestException(val message: String) extends RuntimeException(message) {
    def this() = this("")  
}