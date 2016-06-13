package kafka.message

/**
 * @author zhaori
 */
class InvalidMessageException(message: String) extends RuntimeException(message) {
    def this() = this(null)
    
}