package kafka.network

import kafka.utils.Logging
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import kafka.utils.Utils
/**
 * noted by xinsheng.qiao
 * BoundedByteBufferReceive如何解决粘包问题
 * 首先读取一字节，四bytes的值size作为本次消息的包大小
 * 接着为contentBuffer申请size字节的内存，用于存储该消息
 * 最后从channel中读取内容，直至contentBuffer满为止
 * 
 * 
 * 因一条消息就是一个BoundedByteBufferReceive对象，故sizeBuffer不需要清空
 * @author zhaori
 */
class BoundedByteBufferReceive(val maxSize: Int) extends Receive with Logging {
    private val sizeBuffer = ByteBuffer.allocate(4)
    
    private var contentBuffer: ByteBuffer = null
    
    def this() = this(Int.MaxValue)
    var complete: Boolean = false
    
    /**
    * Get the content buffer for this transmission
    */
    def buffer: ByteBuffer = {
        expectComplete()
        contentBuffer
    }
    
    def readFrom(channel: ReadableByteChannel): Int = {
        exceptionIncomplete()
        var read = 0
        //noted by xinsheng.qiao
        //用4byte存储将要读取消息的大小
        if(sizeBuffer.remaining() > 0)
            read += Utils.read(channel, sizeBuffer)
        /**
         * 此时从channel中读取数据，存在两种情况
         * 1、contentBuffer为null，sizeBuffer接收完全，消息新接收，获取该消息的大小，并为contentBuffer分配内存
         * 2、contentBuffer为null，sizeBuffer未接收完全，下次继续从channel读取数据到sizeBuffer，直至sizeBuffer接收完全
         * 3、contentBuffer不为null，sizeBuffer接收完全，继续未接收完全的消息的接收
         * 4、contentBuffer不为null，sizeBuffer为接收完全，不可能出现的情况
         */
        if(contentBuffer == null && !sizeBuffer.hasRemaining()) {
            //Buffer.rewind()将position设回0，所以你可以重读Buffer中的所有数据。limit保持不变，仍然表示能从Buffer中读取多少个元素（byte、char等）。 
            //接收完全时使用，为从buffer中读取数据做准备
            sizeBuffer.rewind()
            val size = sizeBuffer.getInt()
            if(size <= 0)
                throw new InvalidRequestException("%d is not a valid request size.".format(size))
            if(size > maxSize)
                throw new InvalidRequestException("Request of length %d is not valid, it is larger than the maximum size of %d bytes.".format(size, maxSize))
            contentBuffer = byteBufferAllocate(size)
        }
        //对于上述的判断，如果sizeBuffer仍有剩余，则不会为contentBuffer分配内存，即contentBuffer为null，下次继续从channel读取数据到sizeBuffer，直至sizeBuffer读取成功
        
        
        if(contentBuffer != null) {
            read = Utils.read(channel, contentBuffer)
            //读取完全，未读取完全则下次继续读取
            if(!contentBuffer.hasRemaining()) {
                contentBuffer.rewind()
                complete = true
            }
        }
        
        //返回该次读取的字节数
        read
    }
    
    private def byteBufferAllocate(size: Int): ByteBuffer = {
        var buffer: ByteBuffer = null
        try {
            buffer =ByteBuffer.allocate(size)
        } catch {
            case e: OutOfMemoryError =>
                error("OOME with size " + size, e)
                throw e
            case e2: Throwable =>
                throw e2
        }
        buffer
    }
  
}