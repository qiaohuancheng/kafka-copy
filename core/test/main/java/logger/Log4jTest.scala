package logger
import java.util.ArrayList
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import kafka.utils.Logging
import org.apache.log4j.PropertyConfigurator
import java.io.File
import Utils.Log4jConfig
/**
 * @author zhaori
 */
object Log4jTest extends Logging with Log4jConfig{
    def main(args: Array[String]): Unit = {
        
        LogManager.getCurrentLoggers
        System.out.println(LogManager.getRootLogger.getLevel)
        
        val lst = new ArrayList[String]()
        lst.add("root=" + existingLogger("root").getLevel.toString)
        val loggers = LogManager.getCurrentLoggers
        while (loggers.hasMoreElements) {
            val logger = loggers.nextElement().asInstanceOf[Logger]
            if (logger != null) {
                val level = if (logger != null) logger.getLevel else null
                lst.add("%s=%s".format(logger.getName, if (level != null) level.toString else "null"))
            }
        }
        for(index <- (0 until lst.size())) {
            System.out.println(lst.get(index))
        }
        
        
        
        Thread.sleep(60*60*1000)
    }
    
    def existingLogger(loggerName: String) =
        if (loggerName == "root")
            LogManager.getRootLogger
        else LogManager.getLogger(loggerName)
}