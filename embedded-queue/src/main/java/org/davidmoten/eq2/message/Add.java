package org.davidmoten.eq2.message;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class Add implements Event {
    
    public final Iterable<ByteBuffer> byteBuffers;
    public final CountDownLatch latch;

    public Add(Iterable<ByteBuffer> byteBuffers) {
        this.byteBuffers = byteBuffers;
        this.latch = new CountDownLatch(1);
    }
}
