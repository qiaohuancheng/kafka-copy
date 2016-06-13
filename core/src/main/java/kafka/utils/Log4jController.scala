package kafka.utils

import java.util
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import java.lang.ref.ReferenceQueue.Null
import org.apache.log4j.Level

/**
 * @author zhaori
 */
//noted by xinsheng.qiao
//1. 首先我们在App类中向ManagementFactory申请了一个MBeanServer对象
//2. 接着我们即然要使Echo的实例对象被管理起来，我们就需要给这个对象一个标识，这个标识是ObjectName.注意这个ObjectName构造函数，这里使用了(包名:type=类名)的形式.
//3. 然后我们通过mbs.registerMBean方法注册了echo，并传入了ObjectName在MBeanServer中标识该MBean.
//具体见链接http://blog.csdn.net/qiao000_000/article/details/6061808
//在windows下进入控制台（win+r->cmd），然后输入jconsole命令，稍等片刻打开jconsole的图形界面，在“本地”中选择刚才运行的程序，然后进入MBean面板页，即可看到MyappMBean一项，下面就是具体的MBean，可展开这些MBean对其操作。
object Log4jController {
    private val controller = new Log4jController
    Utils.registerMBean(controller, "kafak:type=kafka.Log4jController")
}

private class Log4jController extends Log4jControllerMBean {
    
    def getLoggers = {
        val lst = new util.ArrayList[String]()
        lst.add("root=" + existingLogger("root").getLevel.toString)
        val loggers = LogManager.getCurrentLoggers
        while (loggers.hasMoreElements) {
            val logger = loggers.nextElement().asInstanceOf[Logger]
            if (logger != null) {
                val level = if (logger != null) logger.getLevel else null
                lst.add("%s=%s".format(logger.getName, if (level != null) level.toString else "null"))
            }
        }
        lst
    }
    
    private def newLogger(loggerName: String) =
        if (loggerName == "root")
            LogManager.getRootLogger
        else LogManager.getLogger(loggerName)
    
    private def existingLogger(loggerName: String) =
        if (loggerName == "root")
            LogManager.getRootLogger
        else LogManager.getLogger(loggerName)
        
    def getLogLevel(loggerName: String) = {
        val log = existingLogger(loggerName)
        if (log != null) {
            val level = log.getLevel
            if (level != null)
                log.getLevel.toString
            else "Null log level."
        }
        else "No such logger"
    }
    def setLogLevel(loggerName: String, level: String) = {
        val log = newLogger(loggerName)
        if(!loggerName.trim.isEmpty && !level.trim.isEmpty && log != null) {
            log.setLevel(Level.toLevel(level.toUpperCase))
            true
        }
        else false
    }
}
//noted by xinsheng.qiao
//Introspector、MBeanIntrospector：JMX用这两个类进行内省，即通过他们能分析MBean的所有属性、方法，然后进行封装、转化、存储等，转化成我们需要的数据结构
//推测：函数名为getXXX且无参数的，其结果会显示在jconsole的属性中，其它的则显示在jconsole的操作中
private trait Log4jControllerMBean {
    def getLoggers: java.util.List[String]
    def getLogLevel(logger: String): String
    def setLogLevel(logger: String, level: String): Boolean
}