package kafka.utils

import java.lang.management.ManagementFactory
import javax.management.ObjectName
import scala.util.control.Exception
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.io.EOFException

/**
 * @author zhaori
 */
object Utils extends Logging{
    def registerMBean(mbean: Object, name: String): Boolean = {
        try {
            val mbs =ManagementFactory.getPlatformMBeanServer()
            mbs synchronized {
                val objName = new ObjectName(name)
                if (mbs.isRegistered(objName))
                    mbs.unregisterMBean(objName)
                mbs.registerMBean(mbean, objName)
                true
            } 
        }catch {
            case e: Exception => {
                error("Failed to register Mbean " + name, e)
                false
            }
        }
    }
    
    def newThread(name: String, runnable: Runnable, daemon: Boolean): Thread = {
        val thread = new Thread(runnable, name)
        thread.setDaemon(daemon)
        
        //note by xinsheng.qiao
        //用于处理线程运行期间未检测的异常，当处理该异常时，会进行相关的处理，并结束当前线程
        //具体见http://www.importnew.com/14434.html
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            def uncaughtException(t: Thread, e: Throwable) {
                error("Uncaught exception in thread '" + t.getName + "':",e)
            }
        })
        thread
    }
    
    def unregisterMBean(name: String) {
        val mbs = ManagementFactory.getPlatformMBeanServer
        mbs synchronized {
            val objName = new ObjectName(name)
            if (mbs.isRegistered(objName))
                mbs.unregisterMBean(objName)
        }
    }
    
    def swallow(log: (Object, Throwable) => Unit, action: => Unit) {
        try {
            action
        } catch {
            case e: Throwable => log(e.getMessage, e)
        }
    }
    
    def writeUnsignedInt(buffer: ByteBuffer, index: Int, value: Long): Unit = 
        buffer.putInt(index, (value & 0xffffffffL).asInstanceOf[Int])
        
    def crc32(bytes: Array[Byte], offset: Int, size: Int): Long = {
        val crc = new Crc32()
        crc.update(bytes, offset, size)
        crc.getValue()
    }
    
    def hashcode(as: Any *): Int = {
        if(as == null)
            return 0
        var h = 1
        var i = 0
        while (i < as.length) {
            if(as(i) != null) {
                h = 31 * h + as(i).hashCode
                i += 1
            }
        }
        return h
    }
    
    def read(channel: ReadableByteChannel , buffer: ByteBuffer): Int = {
        channel.read(buffer) match {
            case -1 => throw new EOFException("Received -1 when reading from channel, socket has likely been closed.")
            case n: Int => n
        }
    }
}