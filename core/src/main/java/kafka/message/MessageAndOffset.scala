package kafka.message

import com.sun.scenario.effect.Offset

/**
 * @author zhaori
 */
case class MessageAndOffset(message: Message, offset: Long) {
    def nextOffset: Long = offset + 1
}