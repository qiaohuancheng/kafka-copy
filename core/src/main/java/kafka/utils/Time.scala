package kafka.utils

/**
 * @author zhaori
 */
object Time {
    val NsPerUs = 1000
    val UsPerMs = 1000
    val MsPerSec = 1000
    val NsPerMs = NsPerUs * UsPerMs
    val NsPerSec = NsPerMs * MsPerSec
    val UsPerSec = UsPerMs * MsPerSec
    val SecsPerMin = 60
    val MinsPerHour = 60
    val HoursPerDay = 24
    val SecsPerHour = SecsPerMin * MinsPerHour
    val SecsPerDay = SecsPerHour * HoursPerDay
    val MinsPerDay = MinsPerHour * HoursPerDay
}

trait Time {
    def milliseconds: Long
    def nanaseconds: Long
    def sleep(ms: Long)
}

object SystemTime extends Time {
    def milliseconds: Long = System.currentTimeMillis
    def nanaseconds: Long = System.nanoTime
    def sleep(ms: Long): Unit = Thread.sleep(ms)
}