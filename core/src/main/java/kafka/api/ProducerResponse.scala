package kafka.api

import kafka.common.TopicAndPartition
import scala.collection.Map
import kafka.common.ErrorMapping
import kafka.api.ApiUtils._
import java.nio.ByteBuffer
import kafka.common.TopicAndPartition
import kafka.common.TopicAndPartition

/**
 * @author zhaori
 */
object ProducerResponse {
    
}
case class ProducerResponseStatus(var error: Short, offset: Long)

case class ProducerResponse(correlationId: Int,
        status: Map[TopicAndPartition, ProducerResponseStatus]) 
        extends RequestOrResponse() {
    
    private lazy val statusGroupedByTopic = status.groupBy(_._1.topic)
    
    def hasError = status.values.exists(_.error != ErrorMapping.NoError)
    
    val sizeInBytes = {
        val groupedStatus = statusGroupedByTopic
        4 +
        4 +
        groupedStatus.foldLeft(0)((foldedTopics, currTopic) => {
            foldedTopics +
            shortStringLength(currTopic._1) +
            4 +
            currTopic._2.size * {
                4 +
                2 +
                8
            }
        })
    }
    
    def writeTo(buffer: ByteBuffer) {
        val groupedStatus = statusGroupedByTopic
        buffer.putInt(correlationId)
        buffer.putInt(groupedStatus.size)
        
        groupedStatus.foreach(topicStatus => {
            val (topic, errorsAndOffsets) = topicStatus
            writeShortString(buffer, topic)
            buffer.putInt(errorsAndOffsets.size)
            errorsAndOffsets.foreach {
                case((TopicAndPartition(_, partition), ProducerResponseStatus(error, nextOffset))) =>
                    buffer.putInt(partition)
                    buffer.putShort(error)
                    buffer.putLong(nextOffset)
            }
        })
    }
    
     override def describe(details: Boolean): String = {toString}
    
}