package selector

import java.nio.channels.Selector
import java.nio.channels.SelectionKey
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.nio.channels.ServerSocketChannel
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.net.InetAddress
import Utils.Env
import scala.util.Random

/**
 * @author zhaori
 */
object SelectorServerTest extends Env{
    
    def main(args: Array[String]) {
        
        var index = new AtomicLong
        
        var receivebuffer = ByteBuffer.allocate(4);
        
        val serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(8080))
        
        val selector = Selector.open();
        val selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
        
        info("server start")
        
        var server: ServerSocketChannel = null
        var client: SocketChannel = null
        
        while(true) {
            var ready = selector.select(300)
            val keys = selector.selectedKeys()
            
            val iter = keys.iterator()
            while(iter.hasNext()) {
                var key: SelectionKey = null
                key = iter.next()
                iter.remove()
                    
                if (key.isReadable() || key.isWritable()) {
                    info("isReadable or isWritable")
                    var random =Random.nextInt()
                    random = 0
                    if(random % 2 == 0) {
                        client = key.channel().asInstanceOf[SocketChannel]
                        //将缓冲区清空以备下次读取  
                        receivebuffer.clear();
                        //读取服务器发送来的数据到缓冲区中  
                        var count=client.read(receivebuffer);
                        if (count > 0) {  
                            var temp = new String(receivebuffer.array(),0,count).toLong
                            info("服务器接收客户端数据--:"+temp); 
                            index.getAndSet(temp)
                        }
                    } else {
                        
                        client = key.channel().asInstanceOf[SocketChannel]
                        var writeBuffer = ByteBuffer.wrap(index.getAndIncrement.toString().getBytes()); 
                        info("write " + writeBuffer + " to client" )
                        client.write(writeBuffer); 
                    }
                } else if (key.isAcceptable()) {
                    info("isAcceptable")
//                    Thread.sleep(10 * 60 * 60 *1000)
                    server = key.channel().asInstanceOf[ServerSocketChannel]
                    // 此方法返回的套接字通道（如果有）将处于阻塞模式。  
                    client = server.accept();  
                    // 配置为非阻塞  
                    client.configureBlocking(false);  
                    // 注册到selector，等待连接  
                    client.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    info("connect from " + client.getRemoteAddress + " to " + client.getLocalAddress + " is established.")
                }
                else if(!key.isValid())
                    serverSocketChannel.close()
            }
        }
    }
}