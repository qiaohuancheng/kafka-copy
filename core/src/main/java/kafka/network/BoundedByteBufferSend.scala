package kafka.network

import java.nio.ByteBuffer
import kafka.api.RequestOrResponse
import java.nio.channels.GatheringByteChannel
/**
 * @author zhaori
 */
class BoundedByteBufferSend(val buffer: ByteBuffer) extends Send {
    
    private var sizeBuffer = ByteBuffer.allocate(4)
    
    var complete: Boolean = false
    //在scala中，构造函数的重载和普通函数的重载是基本一样的，区别只是构造函数使用this关键字指代！当然，也不能指定返回值。
    //对于重载构造函数：它的第一个语句必须是调用另外一个重载的构造函数或者是主构造函数！当然除了主构造函数以外！
    def this(size: Int) = this(ByteBuffer.allocate(size))
    //RequestOrResponse 各类request和response的超类
    //应该broker收到的所有消息都有requestId
    def this(request: RequestOrResponse) = {
        this(request.sizeInBytes + (if(request.requestId != None) 2 else 0))
        request.requestId match {
            case Some(requestId) => buffer.putShort(requestId)
            case None =>
        }
    }
    
    //BoundedByteBufferSend主备就绪，即
    def writeTo(channel: GatheringByteChannel): Int = {
        //BoundedByteBufferSend主备就绪，即sizeBuffer和buffer中均已存在数据，且数据正常
        expectComplete()
        //Writes a sequence of bytes to this channel from the given buffers.
        var written = channel.write(Array(sizeBuffer, buffer))
        if(!buffer.hasRemaining)
            complete = true
        written.asInstanceOf[Int]
    }
    
  
}