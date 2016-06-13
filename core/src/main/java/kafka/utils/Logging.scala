package kafka.utils

import org.apache.log4j.Logger

/**
 * @author zhaori
 */
trait Logging {
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)
     
    protected var logIdent: String = null
       
    // Force initialization to register Log4jControllerMBean
    private val log4jController = Log4jController 
    
    private def msgWithLogIdent(msg: String) =
        if(logIdent == null) msg else logIdent + msg
        
    //noted by xinsheng.qiao
    //传名调用（msg: => String），传名调用和传值调用的区别：
    //传值调用在进入函数体之前就对参数表达式进行了计算，这避免了函数内部多次使用参数时重复计算其值，在一定程度上提高了效率。
    //但是传名调用的一个优势在于，如果参数在函数体内部没有被使用到，那么它就不用计算参数表达式的值了。在这种情况下，传名调用的效率会高一点。
    def trace(msg: => String): Unit = {
        if (logger.isTraceEnabled()) 
            logger.trace(msgWithLogIdent(msg))
    }
    
    def trace(e: => Throwable): Any = {
        if (logger.isTraceEnabled())
            logger.trace(logIdent, e)
    }
    
    def trace(msg: => String, e: => Throwable) {
        if (logger.isTraceEnabled())
            logger.trace(msg, e)
    }
    
    def swallowTrace(action: => Unit) {
        Utils.swallow(logger.trace, action)
    }
    
    def debug(msg: => String): Unit = {
        if (logger.isDebugEnabled())
            logger.debug(msgWithLogIdent(msg))
    }
    
    def debug(e: => Throwable): Any = {
        if (logger.isDebugEnabled())
            logger.debug(logIdent,e)
    }
    
    def debuf(msg: => String, e: => Throwable) {
        if (logger.isDebugEnabled()) 
            logger.debug(msgWithLogIdent(msg),e)
    }
    
    def swallowDebug(action: => Unit) {
        Utils.swallow(logger.debug, action)
    }
    
    def info(msg: => String): Unit = {
        if (logger.isInfoEnabled())
            logger.info(msgWithLogIdent(msg))
    }
    
    def info(e: => Throwable): Any = {
        if (logger.isInfoEnabled())
            logger.info(logIdent,e)
    }
    
    def info(msg: => String,e: => Throwable) = {
        if (logger.isInfoEnabled())
            logger.info(msgWithLogIdent(msg),e)
    }
    
    def swallowInfo(action: => Unit) {
        Utils.swallow(logger.info, action)
    }
    
    def warn(msg: => String): Unit = {
        logger.warn(msgWithLogIdent(msg))
    }
    
    def warn(e: => Throwable): Any = {
        logger.warn(logIdent,e)
    }
    
    def warn(msg: => String, e: => Throwable) = {
        logger.warn(msgWithLogIdent(msg),e)
    }
  
    def swallowWarn(action: => Unit) {
        Utils.swallow(logger.warn, action)
    }
    
    def swallow(action: => Unit) = swallowWarn(action)
    
    def error(msg: => String): Unit = {
        logger.error(msgWithLogIdent(msg))
    }
    
    def error(e: => Throwable): Any = {
        logger.error(logIdent,e)
    }
    
    def error(msg: => String, e: => Throwable) = {
        logger.error(msgWithLogIdent(msg), e)
    }
    
    def swallowError(action: => Unit) {
        Utils.swallow(logger.error, action)
    }

    def fatal(msg: => String): Unit = {
        logger.fatal(msgWithLogIdent(msg))
    }
    
    def fatal(e: => Throwable): Any = {
        logger.fatal(logIdent,e)
    }
    
    def fatal(msg: => String, e: => Throwable) = {
        logger.fatal(msgWithLogIdent(msg),e)
    }
}