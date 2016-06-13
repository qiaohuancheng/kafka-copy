package kafka.message

import java.nio._
import kafka.utils.Utils
import scala.math._

/**
 * @author zhaori
 */
object Message {
    val CrcOffset = 0
    val CrcLength = 4
    val MagicOffset = CrcOffset + CrcLength
    val MagicLength = 1
    val AttributesOffset = MagicOffset + MagicLength
    val AttributesLength = 1
    val KeySizeOffset = AttributesOffset + AttributesLength
    val KeySizeLength = 4
    val KeyOffset = KeySizeOffset + KeySizeLength
    val ValueSizeLength = 4
    
    val MessageOverhead = KeyOffset + ValueSizeLength
    
    val MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength
    
    val CurrentMaginValue: Byte = 0
    
    val CompressionCodeMask: Int = 0x07
    
    val NoCompression: Int = 0
    
}

class Message(val buffer: ByteBuffer) {
    import kafka.message.Message._
    
    def this(bytes: Array[Byte],
            key: Array[Byte],
            codec: CompressionCodec,
            payloadOffset: Int,
            payloadSize: Int) = {
        this(ByteBuffer.allocate(Message.CrcLength +
                Message.MagicLength +
                Message.AttributesLength + 
                Message.KeySizeLength +
                (if(key == null) 0 else key.length) + 
                Message.ValueSizeLength +
                (if(bytes == null) 0 
                else if (payloadSize >= 0) payloadSize 
                else bytes.length - payloadOffset)))
         buffer.position(MagicOffset)
         buffer.put(CurrentMaginValue)
         var attributes: Byte = 0
         if (codec.codec > 0)
             attributes = (attributes | (CompressionCodeMask & codec.codec)).toByte
         buffer.put(attributes)
         if (key == null) {
             buffer.putInt(-1)
         } else {
             buffer.putInt(key.length)
             buffer.put(key, 0, key.length)
         }
        val size = if(bytes == null) -1
                    else if (payloadSize >= 0) payloadSize
                    else bytes.length - payloadSize
        buffer.putInt(size)
        if (bytes != null)
            buffer.put(bytes, payloadOffset, size)
        buffer.rewind()
        Utils.writeUnsignedInt(buffer, CrcOffset, computeChecksum)
    }
    
    def this(bytes: Array[Byte], key: Array[Byte], codec: CompressionCodec) = 
        this(bytes = bytes, key = key, codec = codec, payloadOffset = 0, payloadSize = -1)
    
    def this(bytes: Array[Byte], codec: CompressionCodec) =
        this(bytes = bytes, key = null, codec = codec)
    
    def computeChecksum(): Long = 
        Utils.crc32(buffer.array, buffer.arrayOffset + MagicOffset, buffer.limit - MagicOffset)
    
    def size: Int = buffer.limit
    
    def keySize: Int = buffer.getInt(Message.KeySizeOffset)
    private def payloadSizeOffset = Message.KeyOffset + max(0, keySize)
    
    def compressionCodec: CompressionCodec = 
        CompressionCodec.getCompressionCodec(buffer.get(AttributesOffset) & CompressionCodeMask)

    def payload: ByteBuffer = sliceDelimited(payloadSizeOffset)
    
    private def sliceDelimited(start: Int): ByteBuffer = {
        val size = buffer.getInt(start)
        if(size < 0) {
            null 
        } else {
            var b = buffer.duplicate
            b.position(start + 4)
            b = b.slice()
            b.limit(size)
            b.rewind
            b
        }
    }
}
