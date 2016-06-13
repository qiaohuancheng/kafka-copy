package kafka.utils

import kafka.common._
import scala.collection._
import util.parsing.json.JSON
/**
 * @author zhaori
 */
object Json extends Logging{
    
    val lock = new Object
    
    def parseFull(input: String): Option[Any] = {
        lock synchronized {
            try {
                JSON.parseFull(input)
            } catch {
                case t: Throwable =>
                    throw new KafkaException("Can't parse json string: %s".format(input), t)
            }
        }
        
    }
    
}