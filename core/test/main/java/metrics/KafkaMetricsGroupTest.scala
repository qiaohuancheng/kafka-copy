package metrics
import kafka.metrics.KafkaMetricsGroup
import Utils.Log4jConfig
import Utils.Env
import scala.collection.immutable
import com.yammer.metrics.core.Gauge
import com.yammer.metrics.core.Metric
import com.yammer.metrics.Metrics; 
import java.util.concurrent.ArrayBlockingQueue
import com.yammer.metrics.reporting.ConsoleReporter;
import java.util.concurrent.TimeUnit;
import com.yammer.metrics.core.MetricName

/**
 * @author zhaori
 */
object KafkaMetricsGroupTest extends Env with KafkaMetricsGroup{
    def main(args: Array[String]) {

        ConsoleReporter.enable(30,TimeUnit.SECONDS);
        
        for(i <- 0 until 1)
            newMeter("IdlePercent", "percent", TimeUnit.NANOSECONDS, Map("networkProcessor" -> i.toString))
        Thread.sleep(60*60*1000)
//        val test = new Test
//        var a = 0
//        while( true ){
//            test.put("sinosun")
//            Thread.sleep(1000)
//            a = a + 1;
//            if(a == 30)
//                test.remove(a)
//
//        }
    }
}

private class Test extends KafkaMetricsGroup {
    val requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS)
    val requestQueue = new ArrayBlockingQueue[String](1000)
    val requestQueueTimeHist = newHistogram("RequestQueueTimeMs", biased = true)
//    val timer = newTimer("FetchRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
//    val timer = Metrics.newTimer(new MetricName("sinosun", "s", "ss"), TimeUnit.MILLISECONDS,TimeUnit.SECONDS);  

    newGauge(
        "RequestQueueSize",
        new Gauge[Int] {
            def value = requestQueue.size
        }
    )
    def put(str: => String) : Unit = {
        requestQueue.put(str)
        requestRate.mark()
        requestQueueTimeHist.update(10)
    }
    def remove(index: => Int ){
        System.out.println("remove RequestsPerSec");
        removeMetric("RequestsPerSec");
    }
}