package kafka.message

import java.util.stream.Stream
import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import org.xerial.snappy.SnappyInputStream
import java.io.InputStream
import java.util.zip.GZIPInputStream

/**
 * @author zhaori
 */
object CompressionFactory {
    def apply(compressionCodec: CompressionCodec, stream: OutputStream): OutputStream = {
        compressionCodec match {
            case DefaultCompressionCodec => new GZIPOutputStream(stream)
            case GZIPCompressionCodec => new GZIPOutputStream(stream)
            case SnappyCompressionCodec =>
                import org.xerial.snappy.SnappyOutputStream
                new SnappyOutputStream(stream)
//            case LZ4CompressionCodec => new kafkaLZ4BlockOutputStream(stream)
            case _ => 
                throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
        }
    }
    
    def apply(compressionCodec: CompressionCodec, stream: InputStream): InputStream = {
        compressionCodec match {
            case DefaultCompressionCodec => new GZIPInputStream(stream)
            case GZIPCompressionCodec => new GZIPInputStream(stream)
            case SnappyCompressionCodec =>
                import org.xerial.snappy.SnappyInputStream
                new SnappyInputStream(stream)
//            case LZ4CompressionCodec => 
////                new kafkaLZ4BlockInputStream(stream)
            case _ => 
                throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
        }
    }
}