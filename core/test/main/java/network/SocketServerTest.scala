package network

import kafka.network.SocketServer
import Utils.Env
import java.util.Map
import java.util.HashMap
import scala.collection.mutable

/**
 * @author zhaori
 */
object SocketServerTest extends Env{
     def main(args: Array[String]) {
        
        val socketServer = new SocketServer(
                0, //brokerId
                "", //host
                9876, //port
                30, //numProcessorThreads
                1024, // maxQueuedRequests
                1024, //sendBufferSize
                1024, //recvBufferSize
                1024, //maxRequestSize
                32, //maxConnectionsPerIp
                10 * 1000 //connectionMaxIdleMs
                );
        socketServer.startup();
     }
}