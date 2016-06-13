package Utils
import org.apache.log4j.PropertyConfigurator
import java.io.File

/**
 * 
 * 加载log4j配置文件log4j.properties,为main函数提供,应避免非main函数所在类继承该trait
 * @author zhaori
 */
trait Log4jConfig {
    val file = System.getProperty("user.dir") + File.separator +"conf" + File.separator + "log4j.properties";
    PropertyConfigurator.configure(file);
}