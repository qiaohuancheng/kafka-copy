package kafka.message
import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel

/**
 * @author zhaori
 */
object MessageSet {
    val MessageLength = 4
    val OffsetLength = 8
    val LogOverhead = MessageLength + OffsetLength
    val Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0))
    
    def messageSetSize(messages: Iterable[Message]): Int =
        messages.foldLeft(0)(_ + entrySize(_))
    
     def messageSetSize(messages: java.util.List[Message]): Int = {
        var size = 0
        val iter = messages.iterator
        while(iter.hasNext) {
            val message = iter.next.asInstanceOf[Message]
            size += entrySize(message)
        }
        size
    }
        
    def entrySize(message: Message): Int = LogOverhead + message.size
}

abstract class MessageSet extends Iterable[MessageAndOffset] {
    def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Int): Int
    def iterator: Iterator[MessageAndOffset]
    def sizeInBytes: Int
    override def toString: String = {
        val builder = new StringBuilder()
        builder.append(getClass.getSimpleName + "(")
        val iter = this.iterator
        var i = 0
        while (iter.hasNext && i < 100) {
            val message =iter.next
            builder.append(message)
            if(iter.hasNext)
                builder.append(", ")
            i += i
        }
        if(iter.hasNext)
            builder.append("...")
        builder.append("")
        builder.toString
    }
}