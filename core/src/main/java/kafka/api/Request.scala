package kafka.api

import kafka.utils.Logging
import java.nio.ByteBuffer
import kafka.network.RequestChannel
/**
 * @author zhaori
 */
object Request {
    val OrdinaryConsumerId: Int = -1
    val DebuggingConsumerId: Int = -2
    def isValidBrokerId(brokerId: Int): Boolean = (brokerId >= 0)
}

abstract class RequestOrResponse(val requestId: Option[Short] = None) extends Logging {
    def sizeInBytes: Int
    def writeTo(buffer: ByteBuffer): Unit
    def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {}
    def describe(details: Boolean): String
}