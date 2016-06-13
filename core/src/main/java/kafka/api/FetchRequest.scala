package kafka.api

import java.nio.ByteBuffer
import kafka.api.ApiUtils._
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.network.RequestChannel
import kafka.common.ErrorMapping
import kafka.message.MessageSet
import kafka.utils.nonthreadsafe
import java.util.concurrent.atomic.AtomicInteger


case class PartitionFetchInfo(offset:Long, fetchSize: Int)
/**
 * @author zhaori
 */
object FetchRequest {
    val CurrentVersion = 0.shortValue
    val DefaultMaxWait = 0
    val DefaultMinBytes = 0
    val DefaultCorrelationId = 0
    
    def readFrom(buffer: ByteBuffer): FetchRequest = {
        val versionId = buffer.getShort
        val correlationId = buffer.getInt
        val clietnId = readShortString(buffer)
        val replicaId = buffer.getInt
        val maxWait = buffer.getInt
        val minBytes = buffer.getInt
        val topicCount = buffer.getInt
        val pairs = (1 to topicCount).flatMap(_=> {
            val topic = readShortString(buffer)
            val partitionCount = buffer.getInt
            (1 to partitionCount).map(_=> {
                val partitionId = buffer.getInt
                val offset = buffer.getLong
                val fetchSize = buffer.getInt
                (TopicAndPartition(topic, partitionId), PartitionFetchInfo(offset,fetchSize))
            })
        })
        FetchRequest(versionId, correlationId, clietnId, replicaId, maxWait, minBytes, Map(pairs:_*))
    }
}
case class FetchRequest(versionId: Short = FetchRequest.CurrentVersion,
                        correlationId: Int = FetchRequest.DefaultCorrelationId,
                        clientId: String = ConsumerConfig.DefaultClientId,
                        replicaId: Int = Request.OrdinaryConsumerId,
                        maxWait: Int = FetchRequest.DefaultMaxWait,
                        minBytes: Int = FetchRequest.DefaultMinBytes,
                        requestInfo: Map[TopicAndPartition, PartitionFetchInfo]) 
                        extends RequestOrResponse(Some(RequestKeys.FetchKey)) {
    lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)
    
    def this(correlationId: Int,
            clientId: String,
            maxWait: Int,
            minBytes: Int,
            requestInfo: Map[TopicAndPartition, PartitionFetchInfo]) {
        this(versionId = FetchRequest.CurrentVersion,
                correlationId = correlationId,
                clientId = clientId,
                replicaId = Request.OrdinaryConsumerId,
                maxWait = maxWait,
                minBytes = minBytes,
                requestInfo = requestInfo)
    }
    
    def writeTo(buffer: ByteBuffer) {
        buffer.putShort(versionId)
        buffer.putInt(correlationId)
        writeShortString(buffer, clientId)
        buffer.putInt(replicaId)
        buffer.putInt(maxWait)
        buffer.putInt(minBytes)
        buffer.putInt(requestInfoGroupedByTopic.size)
        requestInfoGroupedByTopic.foreach {
            case (topic, partitionFetchInfo) => 
                writeShortString(buffer, topic)
                buffer.putInt(partitionFetchInfo.size)
                partitionFetchInfo.foreach {
                    case (TopicAndPartition(_, partition), PartitionFetchInfo(offset, fetchSize)) => 
                        buffer.putInt(partition)
                        buffer.putLong(offset)
                        buffer.putInt(fetchSize)
                }
        }
    }
    
    def sizeInBytes: Int = {
        2 + //versionId
        4 + //correlatitionId
        shortStringLength(clientId) +
        4 + //replicaId
        4 + // maxWait
        4 + // minBytes
        4 + //topic count
        requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
            val (topic, partitionFetchInfos) = currTopic
            foldedTopics +
            shortStringLength(topic) +
            4 +
            partitionFetchInfos.size * (
                    4 +
                    8 +
                    4)
        })
    }
    
    def isFromFollower = Request.isValidBrokerId(replicaId)
    
    def isFromOrdinaryConsumer = replicaId == Request.OrdinaryConsumerId
    
    def numPartitions = requestInfo.size
    
    override def toString(): String = {
        describe(true)
    }
    
    override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
        val fetchResponsePartitionData = requestInfo.map {
            case (topicAndPartition, data) =>
                (topicAndPartition, FetchResponsePartitionData(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), -1 , MessageSet.Empty))
        }
        val errorResponse = FetchResponse(correlationId, fetchResponsePartitionData)
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(errorResponse)))
    }
    
    override def describe(details: Boolean):String = {
        val fetchRequest = new StringBuilder 
        fetchRequest.append("Name: " + this.getClass.getSimpleName)
        fetchRequest.append("; Version: " + versionId)
        fetchRequest.append("; CorrelationId: " + correlationId)
        fetchRequest.append("; ClientId: " + clientId)
        fetchRequest.append("; ReplicaId:" + replicaId)
        fetchRequest.append("; MaxWait: " + maxWait + " ms")
        fetchRequest.append("; MinBytes: " + minBytes + " bytes")
        if(details)
            fetchRequest.append("; RequestInfo:" + requestInfo.mkString(","))
        fetchRequest.toString()
    } 
}

@nonthreadsafe
class FetchRequestBuilder() {
    private val correlationId = new AtomicInteger(0)
    private val versionId = FetchRequest.CurrentVersion
    private var clientId = ConsumerConfig.DefaultClientId
    private var replicaId = Request.OrdinaryConsumerId
    private var maxWait = FetchRequest.DefaultMaxWait
    private var minBytes = FetchRequest.DefaultMinBytes
    private val requestMap = new collection.mutable.HashMap[TopicAndPartition, PartitionFetchInfo]
    
    def addFetch(topic: String, partition: Int, offset: Long, fetchSize: Int) = {
        requestMap.put(TopicAndPartition(topic,partition), PartitionFetchInfo(offset, fetchSize))
        this
    }
    
    def clientId(clientId: String): FetchRequestBuilder = {
        this.clientId = clientId
        this
    }
    
    private[kafka] def replicaId(replicaId: Int): FetchRequestBuilder = {
        this.replicaId =replicaId
        this
    }
    
    def maxWait(maxWait: Int): FetchRequestBuilder = {
        this.maxWait = maxWait
        this
    }
    
    def minBytes(minBytes: Int): FetchRequestBuilder = {
        this.minBytes = minBytes
        this
    }
    
    def build() = {
        val fetchRequest = FetchRequest(versionId, correlationId.getAndIncrement, clientId, replicaId, maxWait, minBytes, requestMap.toMap)
        requestMap.clear()
        fetchRequest
    }
}