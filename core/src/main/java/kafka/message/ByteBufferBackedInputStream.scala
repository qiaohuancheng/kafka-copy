package kafka.message

import java.nio.ByteBuffer

import java.io.InputStream

/**
 * @author zhaori
 */
class ByteBufferBackedInputStream(buffer: ByteBuffer) extends InputStream {
    override def read(): Int = {
        buffer.hasRemaining match {
            case true =>
                (buffer.get() & 0xFF)
            case false => -1
        }
    }
    
    override def read(bytes:Array[Byte], off:Int, len:Int): Int = {
        buffer.hasRemaining match {
            case true => 
                val realLen = math.min(len, buffer.remaining())
                buffer.get(bytes, off, realLen)
                realLen
            case false => -1
        }
    }
}