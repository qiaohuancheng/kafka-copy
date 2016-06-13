package mbean
import java.util
import kafka.utils.Utils
/**
 * @author zhaori
 */
object MBeanTest {
    def main(args: Array[String]): Unit = {
        System.out.println("sinosun");
        var controler = new Controler
        Utils.registerMBean(controler, "kafak:type=kafka.test")
        Thread.sleep(60*60*1000)
    }
}
private class Controler extends ControlerMBean{
     def getServerInfo = {
        val lst = new util.ArrayList[String]()
        lst.add("sinosun no s")
        lst.add("scala no s")
        lst.add("rocketmq no s")
        lst.add("kafka no s")
        lst
    }
    
    def showServerInfos = {
        val lst = new util.ArrayList[String]()
        lst.add("sinosun1 show")
        lst.add("scala show")
        lst.add("rocketmq show")
        lst.add("kafka show")
        lst
    }
    
    def getServerInfos = {
        val lst = new util.ArrayList[String]()
        lst.add("sinosun")
        lst.add("scala")
        lst.add("rocketmq")
        lst.add("kafka")
        lst
    }
    
    def getInfo = {
        "info"
    }
}

//noted by xinsheng.qiao
//Introspector、MBeanIntrospector：JMX用这两个类进行内省，即通过他们能分析MBean的所有属性、方法，然后进行封装、转化、存储等，转化成我们需要的数据结构
//推测：函数名为getXXX且无参数的，其结果会显示在jconsole的属性中，其它的则显示在jconsole的操作中
private trait ControlerMBean {
    def getInfo: String
    def getServerInfo: java.util.List[String]
    def showServerInfos: java.util.List[String]
    def getServerInfos: java.util.List[String]
}