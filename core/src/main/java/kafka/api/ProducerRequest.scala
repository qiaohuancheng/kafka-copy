package kafka.api

import java.nio.ByteBuffer

import kafka.network.RequestChannel
import kafka.api.ApiUtils._
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.common.ErrorMapping
import kafka.network.RequestChannel.Response
import kafka.network.BoundedByteBufferSend
/**
 * @author zhaori
 */
object ProducerRequest {
    var CuerrentVersion = 0.shortValue()
  
    def readFrom(buffer: ByteBuffer): ProducerRequest = {
        val versionId: Short = buffer.getShort
        val correlationId: Int = buffer.getInt
        val clientId: String = readShortString(buffer)
        val requiredAcks: Short = buffer.getShort
        val ackTimeoutMs: Int = buffer.getInt
        //build the topic structure
        val topicCount = buffer.getInt
        val partitionDataPairs = (1 to topicCount).flatMap( _ => {
            //process topic
            val topic = readShortString(buffer)
            val partitionCount = buffer.getInt
            (1 to partitionCount).map(_=> {
                val partition = buffer.getInt
                val messageSetSize = buffer.getInt
                val messageSetBuffer = new Array[Byte](messageSetSize)
                buffer.get(messageSetBuffer, 0, messageSetSize)
                (TopicAndPartition(topic, partition), new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)))
            })
        })
        ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, collection.mutable.Map(partitionDataPairs: _*))
    }
}
case class ProducerRequest(versionId: Short = ProducerRequest.CuerrentVersion,
                           correlationId: Int,
                           clientId: String,
                           requiredAcks: Short,
                           ackTimeoutMs: Int,
                           data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet])
         extends RequestOrResponse(Some(RequestKeys.ProduceKey)) {
        
        private lazy val dataGroupByTopic = data.groupBy(_._1.topic)
        val topicPartitionMessageSizeMap = data.map(r => r._1 -> r._2.sizeInBytes).toMap
        
        def this(correlationId: Int, clientId: String, requiredAcks: Short, ackTimeoutMs: Int,
                 data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) =
                     this(ProducerRequest.CuerrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data)
                    
        
        def writeTo(buffer: ByteBuffer) {
            buffer.putShort(versionId)
            buffer.putInt(correlationId)
            writeShortString(buffer, clientId)
            buffer.putShort(requiredAcks)
            buffer.putInt(ackTimeoutMs)
            
            buffer.putInt(dataGroupByTopic.size)
            
            dataGroupByTopic.foreach{
                case (topic, topicAndPartitionData) =>
                    writeShortString(buffer, topic)
                    buffer.putInt(topicAndPartitionData.size)
                    topicAndPartitionData.foreach(partitionAndData => {
                        val partition = partitionAndData._1.partition
                        val partitionMessageData = partitionAndData._2
                        val bytes = partitionMessageData.buffer
                        buffer.putInt(partition)
                        buffer.putInt(bytes.limit)
                        buffer.put(bytes)
                        bytes.rewind
                })
                
            }
        }
    
    def sizeInBytes: Int = {
        2 +
        4 +
        shortStringLength(clientId) + 
        2 +
        4 + 
        4 +
        dataGroupByTopic.foldLeft(0)((foldedTopics, currTopic) => {
            foldedTopics +
            shortStringLength(currTopic._1) +
            4 +
            {
                currTopic._2.foldLeft(0)((foldedPartitions, currPartition) => {
                    foldedPartitions +
                    4 +
                    4 +
                    currPartition._2.sizeInBytes
                })
            }
        })
        
    }
    def numPartitions = data.size
    
    override def toString(): String = {
        describe(true)
    }
    
    override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
        if(request.requestObj.asInstanceOf[ProducerRequest].requiredAcks == 0) {
            requestChannel.closeConnection(request.processor, request)
        } else {
            val producerReponseStatus = data.map {
                case (topicAndPartition, data) => 
                    (topicAndPartition, ProducerResponseStatus(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]),-1l))
            }
            val errorResponse = ProducerResponse(correlationId, producerReponseStatus)
            requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
            }
        
    }
    override def describe(details: Boolean): String = {
        val producerRequest = new StringBuilder
        producerRequest.append("Name: " + this.getClass.getSimpleName)
        producerRequest.append("; Version: " + versionId)
        producerRequest.append("; CorrelationId: " + correlationId)
        producerRequest.append("; ClientId: " + clientId)
        producerRequest.append("; RequireAcks: " + requiredAcks)
        producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms")
        if(details)
            producerRequest.append("; TopicAndPartition: " + topicPartitionMessageSizeMap.mkString(","))
        producerRequest.toString()   
    }
    
    def emptyData() {
        data.clear()
    }
}