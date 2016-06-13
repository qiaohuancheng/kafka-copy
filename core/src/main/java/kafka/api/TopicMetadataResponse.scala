package kafka.api

import java.nio.ByteBuffer
import kafka.cluster.Broker
/**
 * @author zhaori
 */
object TopicMetadataResponse {
    def readFrom(buffer: ByteBuffer): TopicMetadataResponse = {
        val correlationId = buffer.getInt
        val brokerCount = buffer.getInt
        val brokers = (0 until brokerCount).map(_ => Broker.readFrom(buffer))
        val brokerMap = brokers.map(b => (b.id, b)).toMap
        val topicCount = buffer.getInt
        val topicsMetadata = (0 until topicCount).map(_ => TopicMetadata.readFrom(buffer, brokerMap))
        new TopicMetadataResponse(brokers, topicsMetadata, correlationId)
    }
}
case class TopicMetadataResponse(brokers: Seq[Broker],
                                topicsMetadata: Seq[TopicMetadata],
                                correlationId: Int) extends RequestOrResponse() {
    val sizeInBytes: Int = {
        4 + 4 + brokers.map(_.sizeInBytes).sum + 4 + topicsMetadata.map(_.sizeInBytes).sum
    }
    
    def writeTo(buffer: ByteBuffer) {
        buffer.putInt(correlationId)
        buffer.putInt(brokers.size)
        brokers.foreach(_.writeTo(buffer))
        buffer.putInt(topicsMetadata.length)
        topicsMetadata.foreach(_.writeTo(buffer))
    }
    
    override def describe(details: Boolean): String = { toString }
}