package kafka.common

/**
 * @author zhaori
 */
class BrokerNotAvailableException(message: String) extends RuntimeException(message) {
    def this() = this(null)
  
}