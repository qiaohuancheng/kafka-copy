package org.apache.kafka.common.utils;

public interface Time {
    public long millisenconds();
    public long nanosenconds();
    public void sleep(long ms);
}
