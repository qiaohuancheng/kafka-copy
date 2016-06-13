package kafka.network
import kafka.utils.Utils
import kafka.utils.Logging
import kafka.utils.SystemTime
import kafka.utils.Time
import com.yammer.metrics.core.Meter
import com.yammer.metrics.core.Gauge
import java.util.concurrent._
import java.nio.channels.ServerSocketChannel
import java.net.InetSocketAddress
import java.net.SocketException
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.concurrent.atomic.AtomicBoolean
import java.nio.channels.SocketChannel
import java.net.InetAddress
import scala.collection.mutable
import java.lang.ref.ReferenceQueue.Null
import java.io.EOFException
import kafka.api.RequestKeys
import java.nio.channels.SocketChannel

/**
 * @author zhaori
 */

//noted by xinsheng.qiao
//maxRequestSize干什么用的，怎么用的
//connectionMaxIdleMs干什么用的，怎么用的，作用1：每个链接最大允许的空闲数，即链接的保活间隔，超过该间隔则将链接断开并处理相关的资源；作用2：processor进行maybeCloseOldestConnection的周期
//maxConnectionsPerIp最大连接数的默认值，和maxConnectionsPerIpOverrides相关
//maxConnectionsPerIpOverrides 由broker从配置读取
class SocketServer (val brokerId: Int,
                    val host: String,
                    val port: Int,
                    val numProcessorThreads: Int,
                    val maxQueuedRequests: Int,
                    val sendBufferSize: Int,
                    val recvBufferSize: Int,
                    val maxRequestSize: Int = Int.MaxValue,
                    val maxConnectionsPerIp: Int = Int.MaxValue,
                    val connectionMaxIdleMs: Long,
                    val maxConnectionsPerIpOverrides: Map[String, Int] = Map.empty) extends Logging with KafkaMetricsGroup{
    this.logIdent = "[Socket Server on Broker " + brokerId + "],"
    private val time = SystemTime
    
    //noted by xinsheng.qiao 一个acceptor，多个处理器
    private val processors = new Array[Processor](numProcessorThreads)
    @volatile private var acceptor: Acceptor = null
    
    //noted by xinsheng.qiao
    //RequestChannel只有一个，其中 一个BlockingQueue用于保存request请求，每个处理器一个BlockingQueue，用于保存该处理器需要处理的response
    val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)
    
    //noted by xinsheng.qiao 处理器的空闲百分比
    //作为Processor构造函数的参数，传入每个Processor中
    //问题，百分比是如何计算出来的,percent只是metrics的度量，在显示时是以百分比显示的，但统计时并不是以百分比形式统计的
    //aggregate 总计
    //Percent Gauges
    //跟Ratio Gauge同样的接口，只是将Ratio Gauge以百分比的方式展现。
    private val aggregateIdleMeter = newMeter("NetworkProcessorAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)
    
    def startup() {
        
        //noted by xinsheng.qiao metrics定义中tags的重新认识，只是一组功能相同的事物的统计，只是id不同而已，如Processor，newMeter只是针对该Processor的统计
        //quotas 指标,用于保存每个ip建立的连接数，以及最大允许建立连接数
        val quotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
        for(i <- 0 until numProcessorThreads) {
            processors(i) = new Processor(i,
                                          time,
                                          maxRequestSize,
                                          aggregateIdleMeter,
                                          newMeter("IdlePercent", "percent", TimeUnit.NANOSECONDS, Map("networkProcessor" -> i.toString)),
                                          numProcessorThreads,
                                          requestChannel,
                                          quotas,
                                          connectionMaxIdleMs)
            
            //note by xinsheng.qiao 启动处理器线程
            Utils.newThread("kafka-network-thread-%d-%d".format(port, i), processors(i), false).start()
        }
        
        //note by xinsheng.qiao
        //主要的区别是fold函数操作遍历问题集合的顺序。foldLeft是从左开始计算，然后往右遍历。foldRight是从右开始算，然后往左遍历。而fold遍历的顺序没有特殊的次序。
        //？统计各个处理器上等待发送的Response
        newGauge("ResponsesBeingSend", new Gauge[Int] {
            //noted by xinsheng.qiao 
            //Applies a binary operator to a start value and all elements of this mutable indexed sequence, going left to right.
            //total初始值为0
            def value = processors.foldLeft(0) { (total, p) => total + p.countInterestOps(SelectionKey.OP_WRITE)}
        })
        
        //editor by xinsheng.qiao 只是将(id: Int) => processors(id).wakeup()函数放入addResponseListener 
        requestChannel.addResponseListener((id: Int) => processors(id).wakeup())
        
        this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize, quotas)
        Utils.newThread("kafka-socket-acceptor", acceptor, false).start()
        
        //noted by xinsheng.qiao
        //为什么processor的启动后，没有调用awaitStartup等待启动成功，而acceptor有呢
        acceptor.awaitStartup
        info("Started")
    }
    
    def shtdown() = {
        info("Shutting down")
        if(acceptor != null)
            acceptor.shutdown()
        for(processor <- processors)
            processor.shutdown
        info("Shutdown completed")
    }
  
}

private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {
    
    //noted by xinsheng.qiao
    //通过调用Selector.open()方法创建一个Selector
    //acceptor和processor都会有一个selector
    //Selector（选择器）是Java NIO中能够检测一到多个NIO通道，并能够知晓通道是否为诸如读写事件做好准备的组件。这样，一个单独的线程可以管理多个channel，从而管理多个网络连接。
    protected val selector = Selector.open();
    private val startupLatch = new CountDownLatch(1)
    private val shutdownLatch = new CountDownLatch(1)
    private val alive = new AtomicBoolean(true)
    
    
    /**
     * noted by xinsheng.qiao
     * 由于它代表了一个Server线程，在其内部使用了java.nio的Selector。
     * 所以在shutdown时，需要调用Selector的wakeup方法，使得对Selector的select方法的调用从阻塞中返回。
     * 具体见http://www.cnblogs.com/devos/p/3750565.html
     */
    def shutdown(): Unit = {
        alive.set(false)
        //noted 编译xinsheng.qiao
        //某个线程调用select()方法后阻塞了，即使没有通道已经就绪，也有办法让其从select()方法返回。只要让其它线程在第一个线程调用select()方法的那个对象上调用Selector.wakeup()方法即可。阻塞在select()方法上的线程会立马返回。
        //如果有其它线程调用了wakeup()方法，但当前没有线程阻塞在select()方法上，下个调用select()方法的线程会立即“醒来（wake up）”。
        selector.wakeup()
        shutdownLatch.await
    }
    def awaitStartup(): Unit = startupLatch.await()
    
    protected def startupComplete() = {
        startupLatch.countDown
    }
    
    
    /**
     * noted by xinsheng.qiao
     * 各线程（如acceptor，processor）关闭会调用shutdownComplete
     * socketServer关闭会调用shutdown；
     * 关闭过程为：
     * 1、socketServer调用shutdown；
     * 2、各线程判断alive为false；
     * 3、各线程调用shutdownComplete
     * 
     */
    protected def shutdownComplete() = shutdownLatch.countDown
    
    protected def isRunning = alive.get
    
    /**
     * noted by xinsheng.qiao
     * 调用Selector的wakeup方法，使得对Selector的select方法的调用从阻塞中返回
     */
    def wakeup() {
        info("execute once!!!")
        selector.wakeup()
    }
    
    def close(key: SelectionKey) {
        if(key != null) {
            key.attach(null)
            close(key.channel.asInstanceOf[SocketChannel])
            swallow(key.cancel())
        }
    }
    
    def close(channel: SocketChannel) {
        if(channel != null) {
            debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
            connectionQuotas.dec(channel.socket.getInetAddress)
            swallowError(channel.socket().close())
            swallow(channel.close())
        }
    }
    
    def closeAll() {
        this.selector.selectNow()
        var iter = this.selector.keys.iterator()
        while (iter.hasNext()) {
            val key = iter.next()
            close(key)
        }
    }
    
    def countInterestOps(ops: Int): Int = {
        var count = 0
        val it = this.selector.keys.iterator()
        while (it.hasNext) {
            //noted by xinsheng.qiao
            //
            //用“位与”操作interest 集合和给定的SelectionKey常量，可以确定某个确定的事件是否在interest集合中。
            //写事件已经在这个集合里了
            //写就绪相对有一点特殊，一般来说，你不应该注册写事件。写操作的就绪条件为底层缓冲区有空闲空间，而写缓冲区绝大部分时间都是有空闲空间的，
            //所以当你注册写事件后，写操作一直是就绪的，选择处理线程全占用整个CPU资源。所以，只有当你确实有数据要写时再注册写操作，并在写完以后马上取消注册。
            if((it.next().interestOps() & ops) != 0) {
                count += 1
            }
        }
        count
    }

}

private class Processor(val id: Int,
                      val time: Time,
                      val maxRequestSize: Int,
                      val aggregateIdMeter: Meter,
                      val idleMeter: Meter,
                      val totalProcessorThreads: Int,
                      val requestChannel: RequestChannel,
                      connectionQuotas: ConnectionQuotas,
                      val connectionsMaxIdleMs: Long) extends AbstractServerThread(connectionQuotas) {
    
    //note by xinsheng.qiao 新建连接队列，每个Processor一个，具体如何入队的参看acceptor，
    //acceptor采用轮询的方式进行新连接的入队
    private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
    private val connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000
    private var currentTimeManos = SystemTime.nanaseconds
    //noted by xinsheng.qiao
    //LRU是Least Recently Used 近期最少使用算法；
    //每次读都更新关键字SelectionKey对应的时间戳，
    //在maybeCloseOldestConnection函数中，当时间戳+connectionsMaxIdleNanos<currentTime时，则断开该链接;
    //最长保活时间为connectionsMaxIdleNanos
    private val lruConnections = new java.util.LinkedHashMap[SelectionKey, Long]
    //省去每次的计算
    private var nextIdleCloseCheckTime = currentTimeManos + connectionsMaxIdleNanos
    
    override def run() {
        startupComplete()
        while(isRunning) {
            //noted by xinsheng.qiao
            //用于处理新建的链接
            configureNewConnections()
            processNewResponses()
            
            //noted by xinsheng.qiao processor空闲时间统计
            val startSelectTime = SystemTime.nanaseconds
            var ready = selector.select(300)
            //editor by xinsheng.qiao
            //更新currentTimeManos
            currentTimeManos = SystemTime.nanaseconds
            val idleTime = currentTimeManos - startSelectTime
            idleMeter.mark(idleTime)
            
            
            //每个处理器的空闲时间所有处理器平摊
            aggregateIdMeter.mark(idleTime/totalProcessorThreads)
            
            trace("Processor id " + id + " selection time = " + idleTime + " ns")
            if(ready > 0) {
                val keys = selector.selectedKeys()
                val iter = keys.iterator()
                while(iter.hasNext() && isRunning) {
                    var key: SelectionKey = null
                    try {
                        key = iter.next()
                        iter.remove()
                        if(key.isReadable())
                            read(key)
                        else if (key.isWritable())
                            write(key)
                        else if(!key.isValid())
                            close(key)
                        else 
                            throw new IllegalStateException("Unrecognized key state for processor thread.")
                    } catch {
                        case e: EOFException => {
                            info("Closing socket connection to %s."format(channelFor(key).socket.getInetAddress))
                            close(key)
                        } case e: InvalidRequestException => {
                            info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress))
                        } case e: Throwable => {
                            error("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error",e)
                            close(key)
                        }
                    }
                }
            }
            //根据时间nextIdleCloseCheckTime判断是否该进行检查了
            maybeCloseOldestConnection
        }
        debug("Closing selector.")
        closeAll()
        swallow(selector.close())
        shutdownComplete()
    }
    
    override def close(key: SelectionKey): Unit = {
        lruConnections.remove(key)
        super.close(key)
    }
    
    private def processNewResponses() {
        var curr = requestChannel.receiveResponse(id)
        while(curr != null) {
            //
            val key = curr.request.requestKey.asInstanceOf[SelectionKey]
            try {
                curr.responseAction match {
                    case RequestChannel.NoOpAction => {
                        curr.request.updateRequestMetrics
                        trace("Socket server recvived empty response to send, registering for read: " + curr)
                        key.interestOps(SelectionKey.OP_READ)
                        key.attach(null)
                    }
                    case RequestChannel.SendAction => {
                        trace("Socket server received response to send, registering for write: " + curr)
                        //noted by xinsheng.qiao
                        //指示了应该监听信道上的哪些操作。重点提示：任何对key（信道）所关联的兴趣操作集的改变，都只在下次调用了select()方法后才会生效。
                        key.interestOps(SelectionKey.OP_WRITE)
                        //noted by xinsheng.qiao
                        //将给定的对象附加到此键。
                        //之后可通过 attachment 方法获取已附加的对象。一次只能附加一个对象；调用此方法会导致丢弃所有以前的附加对象。通过附加 null 可丢弃当前的附加对象。
                        key.attach(curr)
                    }
                    case RequestChannel.CloseConnectionAction => {
                        curr.request.updateRequestMetrics
                        trace("Closing socket connection actively according to the response code.")
                        close(key)
                    }
                    case responseCode => throw new KafkaException("No mapping found for response code " + responseCode)
                }
            } catch {
                case e: CancellationException => {
                    debug("Ignoring response for closed socket.")
                    close(key)
                }
            } finally {
                curr = requestChannel.receiveResponse(id)
            }
        }
    }
    
    def accept(socketChannel: SocketChannel) {
        newConnections.add(socketChannel)
        wakeup()
    }
    
    private def configureNewConnections() {
        while(newConnections.size() > 0) {
            val channel = newConnections.poll()
            debug("Processor " + id + " listening to new connection from " + channel.socket().getRemoteSocketAddress)
            
            //noted by xinsheng.qiao 为了将Channel和Selector配合使用，必须将channel注册到selector上
            //只对读事件感兴趣
            //具体参看nio的使用
            channel.register(selector, SelectionKey.OP_READ)
            System.out.println(System.currentTimeMillis() + ":Processor " + id + " listening to new connection from " + channel.socket().getRemoteSocketAddress)
        }
    }
    
    def read(key: SelectionKey) {
        //对于作为attachment的BoundedByteBufferReceive理解有误，BoundedByteBufferReceive为一个对象，初步判断主要是处理粘包问题
        //每次要收新的消息就创建BoundedByteBufferReceive用于存储该消息：
        //1、当该消息从channel中读取完全后，则进行处理，并key.attach(null)
        //2、若消息未接收完全则注册读事件，下次接着接收，直至接收完全进行步骤1的处理
        //BoundedByteBufferReceive如何解决粘包问题
        lruConnections.put(key, currentTimeManos)
        val socketChannel = channelFor(key)
        //noted by xinsheng.qiao
        //该channel非第一次读取到数据了，在第一次时，以为这个可以创建了BoundedByteBufferReceive，并作为这个可以的attachment
        var receive = key.attachment().asInstanceOf[Receive]
        
        //noted by xinsheng.qiao
        //channel建链后第一次读取数据数据，通过 selector.selectedKeys()，走到这一步，肯定这个key的attachment为空
        //新建BoundedByteBufferReceive，并将其作为和这个可以的attachment
        if(key.attachment() == null) {
            receive = new BoundedByteBufferReceive(maxRequestSize)
            key.attach(receive)
        }
        //noted by xinsheng.qiao
        //通过类BoundedByteBufferReceive的readFrom从该channel中读取数据，并将读取结果存储在BoundedByteBufferReceive的contentBuffer中
        //该函数返回从channel中读取的字节数
        val read = receive.readFrom(socketChannel)
        val address = socketChannel.socket.getRemoteSocketAddress
        trace(read + " bytes read from " + address)
        //对于java nio代码，read会返回-1或0或n，如果连接异常则会返回-1
        //The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
        //具体见ReadableByteChannel的javadoc
        //read < 0 表明该channel出现异常了，将其关闭，具体可能导致read小于0的原因见readFrom
        if(read < 0) {
            close(key)
        //读取数据完全
        } else if(receive.complete) {
            //构建request，并入requestQueue队列，等待handle处理该request
            //processor用于表明接收此消息的处理器，用于在消息处理完成后将response添加到消息的response队列中；
            //requestKey则表明发送该response的channel;
            //buffer将要处理的消息byte;
            //remoteAddress作用不明，可能是用于打印吧
            var req = RequestChannel.Request(processor = id, requestKey = key, buffer = receive.buffer, startTimeMs = time.milliseconds, remoteAddress = address)
            requestChannel.sendRequest(req)
            key.attach(null)
            key.interestOps(key.interestOps() & (~SelectionKey.OP_READ))
        } else {
            //读取数据不完全，注册读事件，下一次接着读取
            trace("Did not finish reading, registering for read again on connection " + socketChannel.socket.getRemoteSocketAddress)
            key.interestOps(SelectionKey.OP_READ)
            wakeup()
        }
    }
    
    def write(key: SelectionKey) {
        //结合着消息接收过程，以及read过程，这两句话应该明白了吧
        val socketChannel =channelFor(key)
        val response = key.attachment().asInstanceOf[RequestChannel.Response]
        val responseSend = response.responseSend
        if(responseSend == null)
            throw new IllegalStateException("Registered for write interest but no response attached to key.")
        val written = responseSend.writeTo(socketChannel)
        trace(written + " bytes written to " + socketChannel.socket.getRemoteSocketAddress() + " using key " + key)
        if(responseSend.complete) {
            //发送完成，更新metrics统计，并注册读事件
            response.request.updateRequestMetrics()
            key.attach(null)
            trace("Finished writing, registering for read on connection " + socketChannel.socket.getRemoteSocketAddress())
            key.interestOps(SelectionKey.OP_READ)
        } else {
            //未发送完成，注册写事件，并调用wakeup函数是进程从select的阻塞中唤醒
            trace("Finished writing, registering for read on connection " + socketChannel.socket.getRemoteSocketAddress)
            key.interestOps(SelectionKey.OP_WRITE)
            wakeup()
        }
    }
    
    private def channelFor(key: SelectionKey) = key.channel().asInstanceOf[SocketChannel]
    
    /**
     * 每次可能只关闭lruConnections队列中第一个链接
     * 对于之后的链接在下一次处理时关闭
     */
    private def maybeCloseOldestConnection {
        if(currentTimeManos > nextIdleCloseCheckTime) {
            if(lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeManos + connectionsMaxIdleNanos
            } else {
                val oldestConnectionEntry = lruConnections.entrySet().iterator().next()
                val connectionLastActiveTime = oldestConnectionEntry.getValue
                nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos
                if(currentTimeManos > nextIdleCloseCheckTime) {
                    val key: SelectionKey = oldestConnectionEntry.getKey
                    trace("About to close the idle connection from " + key.channel().asInstanceOf[SocketChannel].socket().getRemoteSocketAddress
                            + " due to being idle for " + (currentTimeManos - connectionLastActiveTime) / 1000 / 1000 + "millis")
                    close(key)
                }
            }
        }
    }
}


/**
 *    getReceiveBufferSize()，setReceiveBufferSize()，getSendBufferSize()，和setSendBufferSize()方法分别用于获取和设置接收发送缓冲区的大小（以字节为单位）。
 * 需要注意的是，这里指定的大小只是作为一种建议给出的，实际大小可能与之存在差异。
 *    还可以在ServerSocket上指定接收缓冲区大小。不过，这实际上是为accept()方法所创建的新Socket实例设置接收缓冲区大小。
 *为什么可以只设置接收缓冲区大小而不设置发送缓冲区的大小呢？当接收了一个新的Socket，它就可以立刻开始接收数据，因此需要在accept()方法完成连接之前设置好缓冲区的大小。
 * 另一方面，由于可以控制什么时候在新接受的套接字上发送数据，因此在发送之前还有时间设置发送缓冲区的大小。
 * 
 * 
 * Acceptor是一个典型NIO 处理新连接的方法类：
 * 它会将新的连接均匀地分配给一个Processor。通过accept方法配置网络参数，并交给processor读写数据。
 */
private[kafka] class Acceptor(val host: String, val port: Int,
        private val processors: Array[Processor],
        val sendBufferSize: Int, val recvBufferSize: Int,
        connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) {
    
    //ServerSocket open
    val serverChannel = openServerSocket(host, port)
    
    def run() {
        //注册监听事件
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        startupComplete()
        
        //对processor轮询，进而将新链接分给各个processor
        var currentProcessor = 0
        while(isRunning) {
            val ready = selector.select(500)
            if(ready > 0) {
                val keys = selector.selectedKeys()
                val iter = keys.iterator
                while(iter.hasNext() && isRunning) {
                    var key: SelectionKey = null
                    try {
                        key = iter.next
                        iter.remove()
                        if(key.isAcceptable)
                            accept(key, processors(currentProcessor))
                        else
                            throw new IllegalStateException("Unrecognized key state for acceptor thread.")
                        
                        //editor by xinsheng.qiao 递增轮询的将新建的链接分到各个processor
                        currentProcessor = (currentProcessor + 1) % processors.length
                    } catch {
                        case e: Throwable => error("Error while accepting connection", e)
                    }
                }
            }
        }
        debug("Closing server socket and selector.")
        swallowError(serverChannel.close())
        swallowError(selector.close())
        shutdownComplete()
            
    }
    
    def openServerSocket(host: String, port: Int): ServerSocketChannel = {
        val socketAddress = 
            if(host == null || host.trim.isEmpty)
                new InetSocketAddress(port)
            else 
                new InetSocketAddress(host, port)
        val serverChannel = ServerSocketChannel.open()
        serverChannel.configureBlocking(false)
        serverChannel.socket().setReceiveBufferSize(recvBufferSize)
        try {
            serverChannel.socket().bind(socketAddress)
            info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, port))
        } catch {
            case e: SocketException => 
                throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostName, port, e.getMessage), e)
        }
        serverChannel
    }
    
    def accept(key: SelectionKey, processor: Processor) {
        val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
        val socketChannel = serverSocketChannel.accept()
        try {
            connectionQuotas.inc(socketChannel.socket().getInetAddress)
            socketChannel.configureBlocking(false)
            socketChannel.socket().setTcpNoDelay(true)
            socketChannel.socket().setSendBufferSize(sendBufferSize)
            
            debug("Accepted connection from %s on %s. sendBufferSize [actual|requestes]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
                    .format(socketChannel.socket().getInetAddress, socketChannel.socket().getLocalAddress,
                            socketChannel.socket().getSendBufferSize, sendBufferSize,
                            socketChannel.socket().getReceiveBufferSize, recvBufferSize))
             processor.accept(socketChannel)
        } catch {
            case e: TooManyConnectionsException =>
                info("Rejected connection from %s, address already has configured maximum of %d connections.".format(e.ip, e.count))
                close(socketChannel)
        }
    }
}
class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {
    private val overrides = overrideQuotas.map(entry => (InetAddress.getByName(entry._1), entry._2))
    private val counts = mutable.Map[InetAddress, Int]()
    
    def inc(addr: InetAddress) {
        counts synchronized {
            val count = counts.getOrElse(addr, 0)
            counts.put(addr, count + 1)
            val max = overrides.getOrElse(addr, defaultMax)
            if(count >= max)
                throw new TooManyConnectionsException(addr, max)
        }
    }
    
    def dec(addr: InetAddress) {
        counts synchronized {
            val count = counts.get(addr).get
            if(count == 1)
                counts.remove(addr)
            else 
                counts.put(addr, count - 1 )
        }
    }
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))