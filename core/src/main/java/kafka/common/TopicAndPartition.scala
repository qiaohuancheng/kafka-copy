package kafka.common

/**
 * @author zhaori
 */
case class TopicAndPartition(topic: String, partition: Int) {
    def this(tuple: (String, Int)) = this(tuple._1, tuple._2)
    //TODO
//    def this(partition: Partition) = this(partition.topic, partition.partitionId)
//    def this(replica: Replica) = this(replica.topic, replica.partitionId)
    def asTuple = (topic, partition)
    override def toString = "[%s,%d]".format(topic, partition)
}