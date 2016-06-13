package Utils

import kafka.utils.Logging

/**
 * @author zhaori
 */
//noted by xinsheng.qiao
trait Env extends Log4jConfig with Logging{
    
    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() = {
            System.out.println("Test is over!")
        }
    })
  
}