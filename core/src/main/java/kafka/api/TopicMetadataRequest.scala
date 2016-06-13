package kafka.api

import kafka.utils.Logging
import java.nio.ByteBuffer
import kafka.api.ApiUtils._
import kafka.network.RequestChannel
import kafka.network.RequestChannel.Response
import kafka.common.ErrorMapping
import kafka.network.BoundedByteBufferSend
import collection.mutable.ListBuffer

/**
 * @author zhaori
 */
object TopicMetadataRequest extends Logging {
    val CurrentVersion = 0.shortValue
    val DefaultClientId = ""
    
    def readFrom(buffer: ByteBuffer): TopicMetadataRequest = {
        val versionId = buffer.getShort
        val correlationId = buffer.getInt
        val clientId = readShortString(buffer)
        val numTopics = readIntInRange(buffer, "number of topics", (0, Int.MaxValue))
        val topics = new ListBuffer[String]()
        for (i <- 0 until numTopics)
            topics += readShortString(buffer)
        new TopicMetadataRequest(versionId, correlationId, clientId, topics.toList)
    }
  
}

case class TopicMetadataRequest(val versionId: Short,
                                val correlationId: Int,
                                val clientId: String,
                                val topics: Seq[String])
                                extends RequestOrResponse(Some(RequestKeys.MetadataKey)) {
    def this(topics: Seq[String], correlationId: Int) = 
        this(TopicMetadataRequest.CurrentVersion, correlationId, TopicMetadataRequest.DefaultClientId, topics)
   
    def writeTo(buffer: ByteBuffer) {
        buffer.putShort(versionId)
        buffer.putInt(correlationId)
        writeShortString(buffer, clientId)
        buffer.putInt(topics.size)
        topics.foreach(topic => writeShortString(buffer, topic))
    }
    
    def sizeInBytes(): Int = {
        2 + //version id
        4 + //correlation id
        shortStringLength(clientId) + //client id
        4 + // number of topics
        topics.foldLeft(0)(_ + shortStringLength(_)) //topics
    }
    
    override def toString(): String = {
        describe(true)
    }
    
    override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
        val topicMetadata = topics.map {
            topic => TopicMetadata(topic, Nil, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
        }
        val errorResponse = TopicMetadataResponse(Seq(), topicMetadata, correlationId)
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
    }
    
    override def describe(details: Boolean): String = {
        val topicMetadataRequest = new StringBuilder
        topicMetadataRequest.append("Name: " + this.getClass.getSimpleName)
        topicMetadataRequest.append("; Version: " + versionId)
        topicMetadataRequest.append("; CorrelationId: " + correlationId) 
        topicMetadataRequest.append("; ClientId: " + clientId)
        if(details)
            topicMetadataRequest.append("; Topics: " + topics.mkString(","))
        topicMetadataRequest.toString()
    }
}