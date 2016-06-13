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
//edit by xinsheng.qiao
//见http://www.cnblogs.com/likwo/archive/2010/06/29/1767814.html
//服务器端的使用ServerSocketChannel，客户端的使用SocketChannel，切记不可弄混
//服务器只有在和客户端建立链接后才能注册SelectionKey.OP_READ和SelectionKey.OP_WRITE
//客户端端可以直接注册SelectionKey.OP_READ和SelectionKey.OP_WRITE
//具体见register的javadoc，javadoc的重要性，遇到新函数要学会看javadoc，不要只知道搜索
//简单的使用例子就是这样
//服务器端绑定后，在与客户端建立链接前，使用getLocalAddress获取到的ip是0.0.0.0，没有例外




object SelectorServer extends Env{
    
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
                if(key.isAcceptable())
                    info("isAcceptable")
                if (key.isReadable())
                    info("isReadable")
                if (key.isWritable())
                    info("isWritable")
                if (key.isValid())
                    info("isValid")
                    
//                if (key.isAcceptable()) {
//                    
//                    server = key.channel().asInstanceOf[ServerSocketChannel]
//                    // 此方法返回的套接字通道（如果有）将处于阻塞模式。  
//                    client = server.accept();  
//                    // 配置为非阻塞  
//                    client.configureBlocking(false);  
//                    // 注册到selector，等待连接  
//                    client.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
//                    info("connect from " + client.getRemoteAddress + " to " + client.getLocalAddress + " is established.")
//                } else if (key.isReadable() || key.isWritable()) {
//                    var random =Random.nextInt()
//                    if(random % 2 == 0) {
//                        client = key.channel().asInstanceOf[SocketChannel]
//                        //将缓冲区清空以备下次读取  
//                        receivebuffer.clear();
//                        //读取服务器发送来的数据到缓冲区中  
//                        var count=client.read(receivebuffer);
//                        if (count > 0) {  
//                            var temp = new String(receivebuffer.array(),0,count).toLong
//                            info("服务器接收客户端数据--:"+temp); 
//                            index.getAndSet(temp)
//                        }
//                    } else {
//                        
//                        client = key.channel().asInstanceOf[SocketChannel]
//                        var writeBuffer = ByteBuffer.wrap(index.getAndIncrement.toString().getBytes()); 
//                        info("write " + writeBuffer + " to client" )
//                        client.write(writeBuffer); 
//                    }
//                }
//                else if(!key.isValid())
//                    serverSocketChannel.close()
            }
        }
    }
}