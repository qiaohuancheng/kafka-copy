package kafka.api

import java.nio.ByteBuffer
import kafka.common.KafkaException

/**
 * @author zhaori
 * 
 * *************************************************************************************************
 * *  RequestKeys.ProduceKey				producer请求					ProducerRequest
 * *************************************************************************************************
 * *  RequestKeys.FetchKey					consumer请求					FetchRequest
 * *************************************************************************************************
 * *  RequestKeys.OffsetsKey				topic的offset请求 				OffsetRequest
 * *************************************************************************************************
 * *  RequestKeys.MetadataKey				topic元数据请求					TopicMetadataRequest
 * *************************************************************************************************
 * *  RequestKeys.LeaderAndIsrKey			leader和isr信息更新请求			LeaderAndIsrRequest
 * *************************************************************************************************
 * *  RequestKeys.StopReplicaKey			停止replica请求				StopReplicaRequest
 * *************************************************************************************************
 * *  RequestKeys.UpdateMetadataKey			更新元数据请求					UpdateMetadataRequest
 * *************************************************************************************************
 * *  RequestKeys.ControlledShutdownKey		controlledShutdown请求		ControlledShutdownRequest
 * *************************************************************************************************
 * *  RequestKeys.OffsetCommitKey			commitOffset请求				OffsetCommitRequest
 * *************************************************************************************************
 * *  RequestKeys.OffsetFetchKey			consumer的offset请求			OffsetFetchRequest
 * *************************************************************************************************
 * 
 */
//noted by xinsheng.qiao
//每个key使用的场景是什么
object RequestKeys {
    val ProduceKey: Short = 0
    val FetchKey: Short = 1
    val OffsetsKey: Short = 2
    val MetadataKey: Short = 3
    val LeaderAndIsrKey: Short = 4
    val StopReplicaKey: Short = 5
    val UpdateMetadataKey: Short = 6
    val ControlledShutdownKey: Short = 7
    val OffsetCommitKey: Short = 8
    val OffsetFetchKey: Short = 9
    val ConsumerMetadataKey: Short = 10
    val JoinGroupKey: Short = 11
    val HeartbeatKey: Short = 12
    
    
    val keyToNameAndDeserializerMap: Map[Short, (String, (ByteBuffer) => RequestOrResponse)] =
        Map(ProduceKey -> ("Produce", ProducerRequest.readFrom),
            FetchKey -> ("Fetch", FetchRequest.readFrom),
//            OffsetsKey -> ("Offsets", OffsetRequest.readFrom)
            MetadataKey -> ("Metadata", TopicMetadataRequest.readFrom)
        )
    
        
    def nameForKey(key: Short): String = {
        keyToNameAndDeserializerMap.get(key) match {
            case Some(nameAndSerializer) => nameAndSerializer._1
            case None => throw new KafkaException("Wrong request type %d".format(key))
        }
    }
    def deserializerForKey(key: Short): (ByteBuffer) => RequestOrResponse = {
        keyToNameAndDeserializerMap.get(key) match {
            case Some(nameAndSerializer) => nameAndSerializer._2
            case None => throw new KafkaException("Wrong request type %d".format(key))
        }
    }
}