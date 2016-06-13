package kafka.network

import kafka.utils.Logging
import kafka.common.KafkaException
import java.nio.channels.GatheringByteChannel
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

/**
 * @author zhaori
 */
private[network] trait Transmission extends Logging{
    def complete: Boolean
    protected def exceptionIncomplete(): Unit = {
    if(complete)
        throw new KafkaException("This operation cannot be completed on a complete request.");
    }
    
    protected def expectComplete(): Unit = {
        if(!complete)
            throw new KafkaException("This operation cannot be completed on an incomplete request.")
    }
}

/**
 * @author zhaori
 */
trait Receive extends Transmission {
    
    def buffer: ByteBuffer
    
    def readFrom(channel:ReadableByteChannel): Int 
    
    def readCompletely(channel: ReadableByteChannel): Int = {
        var totalRead = 0
        while(!complete) {
            val read = readFrom(channel)
            trace(read + " bytes read.")
            totalRead += read
        }
        totalRead
    }
  
}

trait Send extends Transmission {
    def writeTo(channel: GatheringByteChannel): Int
    
    def writeCompletely(channel: GatheringByteChannel): Int = {
        var totalWritten = 0
        while(!complete) {
            val written = writeTo(channel)
            trace(written + " bytes written.")
            totalWritten += written
        }
        totalWritten
    }
}

abstract class MultiSend[S <: Send](val sends: List[S]) extends Send {
    val expectedBytesToWrite: Int
    private var current = sends
    var totalWritten = 0
    
    def writeTo(channel: GatheringByteChannel): Int = {
        expectComplete()
        var totalWrittenPerCall = 0
        var sendComplete: Boolean = false
        do {
            val written = current.head.writeTo(channel)
            totalWritten += written
            totalWrittenPerCall += written
            sendComplete = current.head.complete
            if(sendComplete)
                current = current.tail
        } while(!complete && sendComplete)
        trace("Bytes written as part of multisend call : " + totalWrittenPerCall + 
                "Total bytes written so far : " + totalWritten +
                "Expected bytes to write : " + expectedBytesToWrite )
        totalWrittenPerCall
    }
    
    def complete: Boolean = {
        if(current == Nil) {
            error("mismatch in sending bytes over socket; ecpected: " + expectedBytesToWrite +
                    " actual: " + totalWritten)
            true
        } else {
            false
        }
    }
}