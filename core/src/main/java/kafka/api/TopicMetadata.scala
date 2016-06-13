package kafka.api

import kafka.api.ApiUtils._
import java.nio.ByteBuffer
import kafka.common.ErrorMapping
import kafka.utils.Logging
import kafka.cluster.Broker
import org.apache.kafka.common.utils.Utils._
/**
 * @author zhaori
 */
object TopicMetadata {
    val NoLeaderNodeId = -1
    
    def readFrom(buffer: ByteBuffer, brokers: Map[Int, Broker]): TopicMetadata = {
        val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
        val topic = readShortString(buffer)
        val numPartitions = readIntInRange(buffer, "number of partitions", (0, Int.MaxValue))
        val partitionsMetadata: Array[PartitionMetadata] = new Array[PartitionMetadata](numPartitions)
        for (i <- 0 until numPartitions) {
            val partitionMetadata = PartitionMetadata.readFrom(buffer, brokers)
            partitionsMetadata(partitionMetadata.partitionId) = partitionMetadata
        }
        new TopicMetadata(topic, partitionsMetadata, errorCode)
    }
}
case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMapping.NoError) extends Logging {
    def sizeInBytes: Int = {
        2 + //error code
        shortStringLength(topic) +
        4 + partitionsMetadata.map(_.sizeInBytes).sum // size and partition data array 
    }
    
    def writeTo(buffer: ByteBuffer) {
        buffer.putShort(errorCode)
        writeShortString(buffer, topic)
        buffer.putInt(partitionsMetadata.size)
        partitionsMetadata.foreach(m => m.writeTo(buffer))
    }
    
    override def toString(): String = {
        val topicMetadataInfo = new StringBuilder
        topicMetadataInfo.append("{TopicMetadata for topic %s -> ".format(topic))
        errorCode match {
            case ErrorMapping.NoError => partitionsMetadata.foreach { partitionsMetadata =>
                partitionsMetadata.errorCode match {
                    case ErrorMapping.NoError =>
                        topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                                partitionsMetadata.partitionId, partitionsMetadata.toString()))
                    case ErrorMapping.ReplicaNotAvailableCode =>
                        topicMetadataInfo.append("\nMetadata for partition [%s,%d] is %s".format(topic,
                                partitionsMetadata.partitionId, partitionsMetadata.toString()))
                    case _ =>
                        topicMetadataInfo.append("\nMetadata for partition [%s,%d] is not available due to %s".format(topic,
                                partitionsMetadata.partitionId, ErrorMapping.exceptionFor(partitionsMetadata.errorCode).getClass.getName))
                }
            }
            case _ =>
                topicMetadataInfo.append("\nNo partition metadata for topic %s due to %s".format(topic,
                        ErrorMapping.exceptionFor(errorCode).getClass.getName))
        }
        topicMetadataInfo.append("}")
        topicMetadataInfo.toString()
    }
}

object PartitionMetadata {
    def readFrom(buffer: ByteBuffer, brokers: Map[Int, Broker]): PartitionMetadata = {
        val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue))
        val partitionId = readIntInRange(buffer, "partition id", (0, Int.MaxValue))
        val leaderId = buffer.getInt
        val leader = brokers.get(leaderId)
        
        val numReplicas = readIntInRange(buffer, "number of all replicas", (0, Int.MaxValue))
        val replicaIds = (0 until numReplicas).map(_ => buffer.getInt)
        val replicas = replicaIds.map(brokers)
        
        val numIsr = readIntInRange(buffer, "number of in-sync repicas", (0, Int.MaxValue))
        val isrIds = (0 until numIsr).map(_ => buffer.getInt)
        val isr = isrIds.map(brokers)
        
        new PartitionMetadata(partitionId, leader, replicas, isr, errorCode)
    }
}
case class PartitionMetadata(partitionId: Int,
                            val leader: Option[Broker],
                            replicas: Seq[Broker],
                            isr: Seq[Broker] = Seq.empty,
                            errorCode: Short = ErrorMapping.NoError) extends Logging {
                            
    def sizeInBytes: Int = {
        2 + //error code
        4 + // partition id
        4 + //leader
        4 + 4 * replicas.size + //replica array
        4 + 4 * isr.size //isr array
    }
    
    def writeTo(buffer: ByteBuffer) {
        buffer.putShort(errorCode)
        buffer.putInt(partitionId)
        
        val leaderId = if(leader.isDefined) leader.get.id else TopicMetadata.NoLeaderNodeId
        buffer.putInt(leaderId)
        
        buffer.putInt(replicas.size)
        replicas.foreach(r => buffer.putInt(r.id))
        
        buffer.putInt(isr.size)
        isr.foreach(r => buffer.putInt(r.id))
    }
    
    override def toString(): String = {
        val partitionMetadataString = new StringBuilder
        partitionMetadataString.append("\tpartition " + partitionId)
        partitionMetadataString.append("\tleader: " + (if(leader.isDefined) formatBroker(leader.get) else "none"))
        partitionMetadataString.append("\treplicas: " + replicas.map(formatBroker).mkString(","))
        partitionMetadataString.append("\tisr: " + isr.map(formatBroker).mkString(","))
        partitionMetadataString.append("\tisUnderReplicated: %s".format(if(isr.size < replicas.size) "true" else "false"))
        partitionMetadataString.toString()
    }
    
    private def formatBroker(broker:Broker) = broker.id + "(" + formatAddress(broker.host, broker.port) + ")"
}