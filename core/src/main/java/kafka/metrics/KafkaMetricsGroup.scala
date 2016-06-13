package kafka.metrics

import kafka.utils.Logging
import com.yammer.metrics.core.MetricName
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import scala.collection.immutable
import java.util.concurrent.TimeUnit

/**
 * @author zhaori
 */
trait KafkaMetricsGroup extends Logging{
    private def metricName(name: String, tags: scala.collection.Map[String, String] = Map.empty) = {
        val klass = this.getClass
        val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
        val simpleName = klass.getSimpleName.replace("\\$$", "")
        explicitMetricName(pkg, simpleName, name, tags)
    }
    
    private def explicitMetricName(group: String, typeName: String, name: String, tags: scala.collection.Map[String, String] = Map.empty) = {
        val nameBuilder: StringBuilder = new StringBuilder
        nameBuilder.append(group)
        nameBuilder.append(":type=")
        nameBuilder.append(typeName)
        
        if (name.length > 0) {
            nameBuilder.append(",name=")
            nameBuilder.append(name)
        }
        
        val scope: String = KafkaMetricsGroup.toScope(tags).getOrElse(null)
        //noted by xinsheng.qiao 
        // Option[T] 除了 get() 之外，也提供了另一个叫 getOrElse() 的函式，这个函式正如其名－－如果 Option 里有东西就拿出来，不然就给个默认值。
        //tag name :clietnId=[clientId],用于识别属于不同clientid的metric
        val tagsName = KafkaMetricsGroup.toMBeanName(tags)
        tagsName match {
            case Some(tn) => nameBuilder.append(",").append(tn)
            case None =>
        }
        new MetricName(group, typeName, name, scope, nameBuilder.toString())
    }
    
    //noted by xinsheng.qiao
    //Scala可以指定默认值函数的参数。对于这样的一个参数，可以任选地从一个函数调用，在这种情况下对应的参数将被填充使用默认参数值。
    //为什么这个函数用泛型？因为观测的值不同啊，有int，有long，所以使用泛型很合适啊
    //为什么tags有个默认值，如果在函数中使用默认值会怎么样？具体看tags怎么用的，目前在kafka中tags均采用的是默认值
    
    
    //目前，据观察可知，通过Metrics.defaultRegistry()注册的统计数据，在jconsole可观察到
    //removeMetri后该值即不可再jconsole上观察到，且不对该项进行统计
    def newGauge[T](name: String, metric: Gauge[T], tags: scala.collection.Map[String, String] = Map.empty) =
        Metrics.defaultRegistry().newGauge(metricName(name, tags), metric)

    def newMeter(name: String,  eventType: String, timeUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty) =
        Metrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit)
    
    def newHistogram(name: String, biased: Boolean, tags: scala.collection.Map[String,String] = Map.empty) =
        Metrics.defaultRegistry().newHistogram(metricName(name, tags), biased)
    
    def newTimer(name:String, durationUnit: TimeUnit, rateUnit: TimeUnit, tags: scala.collection.Map[String,String] = Map.empty) = {
        Metrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit)
    }

        
    def removeMetric(name:String, tags: scala.collection.Map[String, String] = Map.empty) =
        Metrics.defaultRegistry().removeMetric(metricName(name, tags))
}

//这个是怎么用的，根据consumerMetricNameList和producerMetricNameList将系统中已经注册的和某个clientId相关的所有监控项删除
//consumerMetricNameList和producerMetricNameList存储的是需要注销的监控项，clientId和具体的producer和consumer相关
//具体见removeAllMetricsInList
object KafkaMetricsGroup extends KafkaMetricsGroup with Logging {
    
   /**
   * To make sure all the metrics be de-registered after consumer/producer close, the metric names should be
   * put into the metric name set.
   */
    private val consumerMetricNameList: immutable.List[MetricName] = immutable.List[MetricName](
            // kafka.consumer.ZookeeperConsumerConnector
            new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "FetchQueueSize"),
            new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "KafkaCommitsPerSec"),
            new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "ZooKeeperCommitsPerSec"),
            new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "RebalanceRateAndTime"),
            new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "OwnedPartitionsCount"),

            // kafka.consumer.ConsumerFetcherManager
            new MetricName("kafka.consumer", "ConsumerFetcherManager", "MaxLag"),
            new MetricName("kafka.consumer", "ConsumerFetcherManager", "MinFetchRate"),

            // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
            new MetricName("kafka.server", "FetcherLagMetrics", "ConsumerLag"),

            // kafka.consumer.ConsumerTopicStats <-- kafka.consumer.{ConsumerIterator, PartitionTopicInfo}
            new MetricName("kafka.consumer", "ConsumerTopicMetrics", "MessagesPerSec"),

            // kafka.consumer.ConsumerTopicStats
            new MetricName("kafka.consumer", "ConsumerTopicMetrics", "BytesPerSec"),

            // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
            new MetricName("kafka.server", "FetcherStats", "BytesPerSec"),
            new MetricName("kafka.server", "FetcherStats", "RequestsPerSec"),
        
            // kafka.consumer.FetchRequestAndResponseStats <-- kafka.consumer.SimpleConsumer
            new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchResponseSize"),
            new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchRequestRateAndTimeMs"),
        
            /**
             * ProducerRequestStats <-- SyncProducer
             * metric for SyncProducer in fetchTopicMetaData() needs to be removed when consumer is closed.
             */
            new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestRateAndTimeMs"),
            new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestSize")
        )
    private val producerMetricNameList: immutable.List[MetricName] = immutable.List[MetricName](
            // kafka.producer.ProducerStats <-- DefaultEventHandler <-- Producer
            new MetricName("kafka.producer", "ProducerStats", "SerializationErrorsPerSec"),
            new MetricName("kafka.producer", "ProducerStats", "ResendsPerSec"),
            new MetricName("kafka.producer", "ProducerStats", "FailedSendsPerSec"),
        
            // kafka.producer.ProducerSendThread
            new MetricName("kafka.producer.async", "ProducerSendThread", "ProducerQueueSize"),
        
            // kafka.producer.ProducerTopicStats <-- kafka.producer.{Producer, async.DefaultEventHandler}
            new MetricName("kafka.producer", "ProducerTopicMetrics", "MessagesPerSec"),
            new MetricName("kafka.producer", "ProducerTopicMetrics", "DroppedMessagesPerSec"),
            new MetricName("kafka.producer", "ProducerTopicMetrics", "BytesPerSec"),
        
            // kafka.producer.ProducerRequestStats <-- SyncProducer
            new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestRateAndTimeMs"),
            new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestSize")
      )
      
    //noted by xinsheng.qiao
    //*************************************************************************************
    //那case classes 又是做什么的呢？ 你可以就把他理解成一个普通的class，但是又略有不同，总结如下：
    //不需要写 new， 但是可以写
    //默认是public ，在任何地方调用
    //默认实现了toString
    //不能被继承
 
    //对case calss的质疑声音比较高，感觉价值不大。官方原文：
    //It makes only sense to define case classes if pattern matching is used to decompose data structures.
 
    //当然，只有在pattern matching下有意义这话未免有所偏激，至少部分老程序员会有其他意见：
    //get auto-generated equals, hashCode, toString, static apply() for shorter initialization, etc.
    //*******************************************************************************************
    //scala语言支持一种叫 map()的动作，这个动作是可以帮你把某个容器的内容，套上一些动作之后，变成另一个新的容器。
    //作为何用
    private def toMBeanName(tags: collection.Map[String, String]): Option[String] = {
        val filteredTags = tags.filter{ case (tagKey, tagValue) => tagValue != ""}
        if (filteredTags.nonEmpty) {
            val tagsString = filteredTags.map{ case (key, value) => "%s=%s".format(key, value)}.mkString(",")
            Some(tagsString)
        } else {
            None
        }
    }
    
    //noted by xinsheng.qiao
    //作为何用
    private def toScope(tags: collection.Map[String, String]): Option[String] = {
        val filteredTags = tags.filter { case (tagKey, tagValue) => tagValue != ""}
        if(filteredTags.nonEmpty) {
            val tagsString = filteredTags.toList.sortWith((t1, t2) => t1._1 < t2._1).map { case (key, value) => "%s.%s".format(key, value.replaceAll("\\.", "_"))}.mkString(".")
            Some(tagsString)
        } else {
            None
        }
    }
    
    def removeAllConsumerMetrics(clientId: String) {
        //TODO copy
        removeAllMetricsInList(KafkaMetricsGroup.consumerMetricNameList, clientId)
    }
    
    def removeAllProducerMetrics(clientId: String) {
        //TODO copy
        removeAllMetricsInList(KafkaMetricsGroup.producerMetricNameList, clientId)
    }
    
    private def removeAllMetricsInList(metricsNameList: immutable.List[MetricName], clientId: String) {
        metricsNameList.foreach ( metric => {
            val pattern = (".*clietnId=" + clientId + ".*").r
            val registeredMetrics = scala.collection.JavaConversions.asScalaSet(Metrics.defaultRegistry().allMetrics().keySet())
            for (registeredMetric <- registeredMetrics) {
                if (registeredMetric.getGroup == metric.getGroup &&
                        registeredMetric.getName == metric.getName &&
                        registeredMetric.getType == metric.getType) {
                    pattern.findFirstIn(registeredMetric.getMBeanName) match {
                        //什么意思？
                        case Some(_) => {
                            val beforeRemovalSize = Metrics.defaultRegistry().allMetrics().keySet().size()
                            Metrics.defaultRegistry().removeMetric(registeredMetric)
                            val afterRemovalSize = Metrics.defaultRegistry().allMetrics().keySet().size()
                            trace("Removing metric %s. Metrics registry size reduced from %d to %d".format(
                                registeredMetric, beforeRemovalSize, afterRemovalSize))
                        }
                        case _ =>
                    }
                }
            }
        })
    }
}