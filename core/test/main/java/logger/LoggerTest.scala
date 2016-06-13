package logger

import kafka.utils.Logging
import org.apache.log4j.PropertyConfigurator
import java.io.File
import Utils.Log4jConfig
import kafka.utils.SystemTime
/*
 * @author zhaori
 */
object LoggerTest extends Logging with  Log4jConfig{
    def main(args: Array[String]): Unit = {
        var currentTimeManos = SystemTime.nanaseconds
        def action(msg: String) {
            trace(msg)
            throw new Throwable(msg)
        }
        
        def getString(msg: => String): String = {
            msg + "sun"
        }
        
        def m(x:Int) = x + "sino"
        
        swallowTrace(action(m(2)));
        
        swallowTrace(action(getString("METHOD")));
    }
}