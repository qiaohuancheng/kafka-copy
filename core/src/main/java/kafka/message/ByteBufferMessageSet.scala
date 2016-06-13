package kafka.message

import java.util.concurrent.atomic.AtomicLong
import java.lang.Class.Atomic
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import kafka.utils.Logging
import java.nio.channels.GatheringByteChannel
import kafka.utils.IteratorTemplate
import java.io.InputStream
import java.lang.ref.ReferenceQueue.Null

/**
 * @author zhaori
 */
object ByteBufferMessageSet {
    private def create(offsetCounter: AtomicLong, compressionCodec: CompressionCodec, messages: Message*): ByteBuffer = {
        if(messages.size == 0) {
            MessageSet.Empty.buffer
        } else if(compressionCodec == NoCompressionCodec) {
            val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
            for(message <- messages)
                writeMessage(buffer, message, offsetCounter.getAndIncrement)
            buffer.rewind
            buffer
        } else {
            val byteArrayStream = new ByteArrayOutputStream(MessageSet.messageSetSize(messages))
            val output = new DataOutputStream(CompressionFactory(compressionCodec, byteArrayStream))
            var offset = -1l
            try {
                for(message <- messages) {
                    offset = offsetCounter.getAndIncrement
                    output.writeLong(message.size)
                    output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
                }
            } finally {
                output.close()
            }
            val bytes = byteArrayStream.toByteArray
            val message = new Message(bytes, compressionCodec)
            val buffer = ByteBuffer.allocate(message.size + MessageSet.LogOverhead)
            writeMessage(buffer, message, offset)
            buffer.rewind()
            buffer
        }
    }
    
    def decompress(message: Message): ByteBufferMessageSet = {
        val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream
        val inputStream: InputStream = new ByteBufferBackedInputStream(message.payload)
        val intermediateBuffer = new Array[Byte](1024)
        val compressed = CompressionFactory(message.compressionCodec, inputStream)
        
        try {
            Stream.continually(compressed.read(intermediateBuffer)).takeWhile(_ > 0).foreach { dataRead =>
                outputStream.write(intermediateBuffer, 0, dataRead)
            }
        } finally {
            compressed.close()
        }
        
        val outputBuffer = ByteBuffer.allocate(outputStream.size)
        outputBuffer.put(outputStream.toByteArray)
        outputBuffer.rewind
        new ByteBufferMessageSet(outputBuffer)
    }
    
    private[kafka] def writeMessage(buffer: ByteBuffer, message: Message, offset: Long) {
        buffer.putLong(offset)
        buffer.putInt(message.size)
        buffer.put(message.buffer)
        message.buffer.rewind()
    }
}

class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet with Logging {
    private var shallowVaildByteCount = -1
    def this(compressionCodec: CompressionCodec, message: Message*) {
        this(ByteBufferMessageSet.create(new AtomicLong(0), compressionCodec, message:_*))
    }
    
    def this(compressionCodec: CompressionCodec, offsetCounter: AtomicLong, messages: Message*) {
        this(ByteBufferMessageSet.create(offsetCounter, compressionCodec, messages:_*))
    }
    
    def this(messages: Message*) {
        this(NoCompressionCodec, new AtomicLong(0), messages:_*)
    }
    
    def writeTo(channel: GatheringByteChannel, offset: Long, size: Int): Int = {
        buffer.mark()
        var written = 0
        while(written < sizeInBytes)
            written += channel.write(buffer)
        buffer.reset()
        written
    }
    
    override def iterator: Iterator[MessageAndOffset] = internalIterator()
    
    private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
        new IteratorTemplate[MessageAndOffset] {
            var topIter = buffer.slice()
            var innerIter: Iterator[MessageAndOffset] = null
            
            def innerDone(): Boolean = (innerIter == null || !innerIter.hasNext)
            
            def makeNextOuter: MessageAndOffset = {
                if(topIter.remaining < 12)
                    return allDone()
                val offset = topIter.getLong
                val size = topIter.getInt
                if(size < Message.MinHeaderSize)
                    throw new InvalidMessageException("Message found with corrupt size (" + size + ")")
                if(topIter.remaining < size)
                    return allDone
                val message = topIter.slice
                message.limit(size)
                topIter.position(topIter.position + size)
                val newMessage = new Message(message)
                
                if(isShallow) {
                    new MessageAndOffset(newMessage, offset)
                } else {
                    newMessage.compressionCodec match {
                        case NoCompressionCodec =>
                            innerIter = null
                            new MessageAndOffset(newMessage, offset)
                        case _ =>
                            innerIter =ByteBufferMessageSet.decompress(newMessage).internalIterator()
                            if(!innerIter.hasNext)
                                innerIter = null
                             makeNext()
                    }
                }
            }
            
            override def makeNext(): MessageAndOffset = {
                if(isShallow) {
                    makeNextOuter
                } else {
                    if(innerDone())
                        makeNextOuter
                    else 
                        innerIter.next
                }
            }
        }
    }
    
    def sizeInBytes: Int = buffer.limit
}