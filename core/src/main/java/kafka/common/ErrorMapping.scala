package kafka.common

import java.nio.ByteBuffer
/**
 * @author zhaori
 */
object ErrorMapping {
    val EmptyByteBuffer = ByteBuffer.allocate(0)
    
    val UnknownCode : Short = -1
    val NoError: Short = 0
    val ReplicaNotAvailableCode: Short = 9
    private val exceptionToCode = 
        Map[Class[Throwable], Short](
                ).withDefaultValue(UnknownCode)
    
    def codeFor(exception: Class[Throwable]): Short = exceptionToCode(exception)
    
    private val codeToException = 
        (Map [Short, Class[Throwable]]() ++ exceptionToCode.iterator.map(p => (p._2, p._1))).withDefaultValue(classOf[UnknownException])
    def exceptionFor(code: Short) : Throwable = codeToException(code).newInstance()
}