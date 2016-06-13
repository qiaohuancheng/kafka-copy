package kafka.message

/**
 * @author zhaori
 */
object CompressionCodec {
    def getCompressionCodec(codec: Int): CompressionCodec = {
        codec match {
            case NoCompressionCodec.codec => NoCompressionCodec
            case GZIPCompressionCodec.codec => GZIPCompressionCodec
            case SnappyCompressionCodec.codec => SnappyCompressionCodec
            case LZ4CompressionCodec.codec => LZ4CompressionCodec
            case _ => throw new kafka.common.UnknownCodecException("%d is an unknown compression codec".format(codec))
        }
    }
    
    def getCompressionCodec(name: String): CompressionCodec = {
        name.toLowerCase match {
            case NoCompressionCodec.name => NoCompressionCodec
            case GZIPCompressionCodec.name => GZIPCompressionCodec
            case SnappyCompressionCodec.name => SnappyCompressionCodec
            case LZ4CompressionCodec.name => LZ4CompressionCodec
            case _ => throw new kafka.common.UnknownCodecException("%d is an unknown compression codec".format(name))
        }
    }
}

sealed trait CompressionCodec { def codec: Int; def name: String }

case object DefaultCompressionCodec extends CompressionCodec {
    val codec = GZIPCompressionCodec.codec
    val name = GZIPCompressionCodec.name
}

case object GZIPCompressionCodec extends CompressionCodec {
    val codec = 1
    val name = "gzip"
}

case object SnappyCompressionCodec extends CompressionCodec {
    val codec = 2
    val name = "snappy"
}

case object LZ4CompressionCodec extends CompressionCodec {
    val codec = 3
    val name = "lz4"
}

case object NoCompressionCodec extends CompressionCodec {
    val codec = 0
    val name = "none"
}