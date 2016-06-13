package org.apache.kafka.common.utils;

public class SystemTime implements Time{

    @Override
    public long millisenconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanosenconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // no stress
        }
    }

}
