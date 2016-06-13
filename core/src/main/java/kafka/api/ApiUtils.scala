package kafka.api

import java.nio._
import java.lang.ref.ReferenceQueue.Null
import kafka.common.KafkaException
import kafka.common.KafkaException
import kafka.common.KafkaException
/**
 * @author zhaori
 */
object ApiUtils {
    val ProtocolEncoding = "UTF-8"
    
    def readShortString(buffer: ByteBuffer): String = {
        val size: Int = buffer.getShort
        if(size < 0)
            return null
         val bytes = new Array[Byte](size)
         buffer.get(bytes)
         new String(bytes, ProtocolEncoding)
    }
    
    def writeShortString(buffer: ByteBuffer, string: String) {
        if(string == null) {
            buffer.putShort(-1)
        } else {
            val encodedString = string.getBytes(ProtocolEncoding)
            if(encodedString.length > Short.MaxValue) {
                throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
            } else {
                buffer.putShort(encodedString.length.asInstanceOf[Short])
                buffer.put(encodedString)
            }
        }
    }
    
    def shortStringLength(string: String): Int = {
        if(string == null) {
            2
        } else {
            val encodedString = string.getBytes(ProtocolEncoding)
            if(encodedString.length > Short.MaxValue) {
                throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
            } else {
                2 + encodedString.length
            }
        }
    }
    
    def readIntInRange(buffer: ByteBuffer, name: String, range: (Int, Int)): Int = {
        val value = buffer.getInt
        if(value < range._1 || value > range._2)
            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
        else 
            value
    }
    
    def readShortInRange(buffer: ByteBuffer, name: String, range: (Short, Short)): Short = {
        val value = buffer.getShort
        if(value < range._1 || value > range._2) 
            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".")
        else value
    }
}