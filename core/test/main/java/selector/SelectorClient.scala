package selector

import java.nio.channels.Selector
import java.nio.channels.SelectionKey
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import Utils.Env
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
/**
 * @author zhaori
 */
object SelectorClient extends Env{
    def main(args: Array[String]): Unit = {
        var receivebuffer = ByteBuffer.allocate(4);
        var index = new AtomicLong
        val sc = SocketChannel.open(); 
        sc.configureBlocking(false); 
        sc.connect(new InetSocketAddress("10.1.1.237", 8080)); 
        val selector = Selector.open(); 
        sc.register(selector, SelectionKey.OP_CONNECT);
        while (true) { 
            selector.select(); 
            var keys = selector.selectedKeys(); 
            var keyIterator = keys.iterator(); 
            while (keyIterator.hasNext()) { 
                var key = keyIterator.next(); 
                keyIterator.remove(); 
                if (key.isConnectable()) { 
                    if (sc.finishConnect()) {
                        sc.register(selector, SelectionKey.OP_WRITE); 
                        info("server connect is sucess!!!"); 
                    }
                } else if (key.isWritable()) { 
                    var writeBuffer = ByteBuffer.wrap(index.getAndIncrement.toString().getBytes()); 
//                    info("write " + writeBuffer + " to server" )
                    sc.write(writeBuffer); 
                } else if (key.isReadable()) {
                    receivebuffer.clear();  
                    //读取服务器发送来的数据到缓冲区中  
                    var count = sc.read(receivebuffer);
                    if (count > 0) {  
                        var temp = new String(receivebuffer.array(),0,count).toLong
                        info("客户端接收服务器端数据--:"+temp); 
                        index.getAndSet(temp)
                    }
                }
            } 
        } 
    }
}