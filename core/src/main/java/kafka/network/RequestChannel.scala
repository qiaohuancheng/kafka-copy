package kafka.network

import kafka.utils.Logging
import kafka.metrics.KafkaMetricsGroup
import java.net._
import java.nio.ByteBuffer
import java.lang.ref.ReferenceQueue.Null
import org.apache.log4j
import org.apache.log4j.Logger
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import kafka.api.RequestOrResponse
import kafka.api.RequestKeys
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.api.ProducerRequest
import kafka.utils.SystemTime
import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue
import com.yammer.metrics.core.Gauge
import kafka.api.FetchRequest


/**
 * RequestChannel是Processor和Handler交换数据的地方。
 * 它包含了一个队列requestQueue用来存放Processor加入的Request，Handler会从里面取出Request来处理；
 * 它还为每个Processor开辟了一个respondQueue，用来存放Handler处理了Request后给客户端的Response。
 * @author zhaori
 */
object RequestChannel extends Logging{
    
    //noted by xinsheng.qiao
    //这个貌似是业务做完或是链接断开时发出的请求，以便broker接收该请求后，进行后续的一些处理;作为客户端链接后发送的最后一条消息
    //具体作用待定？？？
    val AllDone = new Request(1, 2, getShutdownReceive(), 0);
    
    def getShutdownReceive() = {
        val emptyProducerRequest = new ProducerRequest(0, 0, "", 0, 0, collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]())
        val byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes + 2)
        byteBuffer.putShort(RequestKeys.ProduceKey)
        emptyProducerRequest.writeTo(byteBuffer)
        byteBuffer.rewind()
        byteBuffer
    }
    
    
    /**
     * noted by xinsheng.qiao case class
     * http://www.iteblog.com/archives/1508
     * 
     * 1、初始化的时候可以不用new，当然你也可以加上，普通类一定需要加new；
     * 2、toString的实现更漂亮；
     * 3、默认实现了equals 和hashCode；
     * 4、默认是可以序列化的，也就是实现了Serializable ；
     * 5、自动从scala.Product中继承一些函数;
     * 6、case class构造函数的参数是public级别的，我们可以直接访问；
     * 7、支持模式匹配；
     */
    
    //构建request，并入requestQueue队列，等待handle处理该request
    //processor用于表明接收此消息的处理器，用于在消息处理完成后将response添加到消息的response队列中；
    //requestKey则表明发送该response的channel;
    //buffer将要处理的消息byte;
    case class Request(processor: Int, requestKey: Any, private var buffer: ByteBuffer,
            startTimeMs: Long, remoteAddress: SocketAddress = new InetSocketAddress(0)) {
        
        //request出队时间
        @volatile var requestDequeueTimeMs = -1l
        @volatile var apiLocalCompleteTimeMs = -1l
        @volatile var responseCompleteTimeMs = -1l
        @volatile var responseDequeueTimeMs = -1l
        
        //用于识别该消息的类型，具体类型见RequestKeys
        val requestId = buffer.getShort()
        //
        val requestObj: RequestOrResponse = RequestKeys.deserializerForKey(requestId)(buffer)
        //buffer为null，表明该buffer所占内存可以腾出来了
        buffer = null
        private val requestLogger = Logger.getLogger("kafka.request.logger")
        trace("Processor %d received request : %s".format(processor, requestObj))
        
        //何用 待定
        def updateRequestMetrics() {
            val endTimeMs = SystemTime.milliseconds
            
            if(apiLocalCompleteTimeMs < 0)
                apiLocalCompleteTimeMs = responseCompleteTimeMs
            val requestQueueTime = (requestDequeueTimeMs - startTimeMs).max(0L)
            val apiLocalTime = (apiLocalCompleteTimeMs - requestDequeueTimeMs).max(0L)
            val apiRemoteTime = (responseCompleteTimeMs - apiLocalCompleteTimeMs).max(0L)
            val responseQueueTime = (responseDequeueTimeMs - responseCompleteTimeMs).max(0L)
            val responseSendTime = (endTimeMs - responseDequeueTimeMs).max(0L)
            val totalTime = endTimeMs - startTimeMs
            var metricList  = List(RequestMetrics.metricsMap(RequestKeys.nameForKey(requestId)))
            //RequestKeys.FetchKey类型的消息需要增加特别统计
            if(requestId == RequestKeys.FetchKey) {
                val isFromFollower = requestObj.asInstanceOf[FetchRequest].isFromFollower
                metricList ::= ( if (isFromFollower)
                                        RequestMetrics.metricsMap(RequestMetrics.followFetchMetricsName)
                                    else 
                                        RequestMetrics.metricsMap(RequestMetrics.consumerFetchMetricName))
            }
            metricList.foreach{
                m => m.requestRate.mark()
                     m.requestQueueTimeHist.update(requestQueueTime)
                     m.localTimeHist.update(apiLocalTime)
                     m.remoteTimeHist.update(apiRemoteTime)
                     m.responseQueueTimeHist.update(responseQueueTime)
                     m.responseSendTimeHist.update(responseSendTime)
                     m.totalTimeHist.update(totalTime)
            }
          if(requestLogger.isTraceEnabled)
              requestLogger.trace("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d"
                  .format(requestObj.describe(true), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime))
          else if(requestLogger.isDebugEnabled) {
              requestLogger.debug("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d"
                  .format(requestObj.describe(false), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime))
        }
    }
}
    
case class Response(processor: Int, request: Request, responseSend: Send, responseAction: ResponseAction) {
    request.responseCompleteTimeMs = SystemTime.milliseconds
    
    def this(processor: Int, request: Request, responseSend: Send) =
        this(processor, request, responseSend, if(responseSend == null) NoOpAction else SendAction)
        
    def this(request: Request, send: Send) = 
        this(request.processor, request, send)
}
    
    trait ResponseAction
    case object SendAction extends ResponseAction
    case object NoOpAction extends ResponseAction
    case object CloseConnectionAction extends ResponseAction
}

class RequestChannel(val numProcessors: Int,val queueSize: Int) extends KafkaMetricsGroup{
    private var responseListener: List[(Int) => Unit] = Nil
    private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
    private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
    
    //noted by xinsheng.qiao
    //初始化responseQueues，responseQueues为array，其中存储的是BlockingQueue，在上面的程序中array初始化了，但里面的BlockingQueue未初始化，故应添加如下代码对其初始化
    for(i <- 0 until numProcessors)
        responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()

    //noted by xinsheng.qiao RequestQueue的大小
    newGauge("RequestQueueSize", new Gauge[Int] {
        def value = requestQueue.size
    })

    //noted by xinsheng.qiao 所有ResponseQueue的大小
    newGauge("ResponseQueueSize", new Gauge[Int]{
        def value = responseQueues.foldLeft(0) {(total, q) => total + q.size()}
    })

    def closeConnection(processor: Int, request: RequestChannel.Request) {
        responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction))
        for(onResponse <- responseListener)
            onResponse(processor)
    }
    
    def sendRequest(request: RequestChannel.Request) {
        requestQueue.put(request)
    }
    
    def sendResponse(response: RequestChannel.Response) {
        responseQueues(response.processor).put(response)
        for(onResponse <- responseListener)
            onResponse(response.processor)
    }
    
    def receiveResponse(processor: Int): RequestChannel.Response = {
        val response = responseQueues(processor).poll()
        if(response != null)
            response.request.responseCompleteTimeMs = SystemTime.milliseconds
        response
    }
    def addResponseListener(onResponse: Int => Unit) {
        //editor by xinsheng.qiao 
        //或许 List 最常用的操作符是发音为“ cons”的‘ ::’ 。 Cons 把一个新元素组合到已有 List的最前端，然后返回结果 List
        //val twoThree = list(2, 3)
        //val oneTwoThree = 1 :: twoThree
        //println(oneTwoThree)
        //你会看到：
        //List(1, 2, 3)
        
        //onResponse只是一个函数，将该函数放在responseListener最前面
        //onResponse:processors(id).wakeup()
        responseListener ::= onResponse
    }
}

object RequestMetrics {
    val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
    val consumerFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "Consumer"
    val followFetchMetricsName = RequestKeys.nameForKey(RequestKeys.FetchKey) + " Follower"
    
    //noted by xinsheng.qiao 
    //啥用
    (RequestKeys.keyToNameAndDeserializerMap.values.map(e => e._1) ++ List(consumerFetchMetricName, followFetchMetricsName))
    .foreach (name => metricsMap.put(name, new RequestMetrics(name)))
}
class RequestMetrics(name: String) extends KafkaMetricsGroup {
    val tags = Map("request" -> name)
    val requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)
    val requestQueueTimeHist = newHistogram("RequestQueueTimeMs", biased = true, tags)
    val localTimeHist = newHistogram("LocalTimeMs", biased = true , tags)
    val remoteTimeHist = newHistogram("RemoteTimeMs", biased = true, tags)
    val responseQueueTimeHist = newHistogram("ResponseQueueTimeMs", biased = true, tags)
    val responseSendTimeHist = newHistogram("ResponseSendTimeMs", biased = true, tags)
    val totalTimeHist = newHistogram("TotalTimeMs", biased = true, tags)
}