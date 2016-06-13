package kafka.utils

import java.util.NoSuchElementException
import scala.util.control.TailCalls.Done

class State
object DONE extends State
object READY extends State
object NOT_READY extends State
object FAILED extends State

/**
 * @author zhaori
 */
abstract class IteratorTemplate[T] extends Iterator[T] with java.util.Iterator[T] {
    private var state: State = NOT_READY
    private var nextItem = null.asInstanceOf[T]
    
    def next(): T = {
        if(!hasNext())
            throw new NoSuchElementException
        state = NOT_READY
        if(nextItem == null)
            throw new IllegalStateException("Expected item but none found.")
        nextItem
    }
    
    def peek(): T = {
        if(!hasNext())
            throw new NoSuchElementException()
        nextItem
    }
    
    def hasNext(): Boolean = {
        if(state == FAILED)
            throw new IllegalStateException("Iterator is in failed state")
        state match {
            case DONE => false
            case READY => true
            case _ => maybeComputeNext()
        }
    }
    
    protected def makeNext(): T
    
    def maybeComputeNext(): Boolean = {
        state = FAILED
        nextItem = makeNext()
        if(state == DONE) {
            false
        } else {
            state = READY
            true
        }
    }
    
    protected def allDone(): T = {
        state =DONE
        null.asInstanceOf[T]
    }
    
    def remove = 
        throw new UnsupportedOperationException("Removal not supported")
    
    protected def resetState() {
        state = NOT_READY
    }

}