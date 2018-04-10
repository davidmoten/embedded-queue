package org.davidmoten.eq2.event;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class AddMessage implements Event {
    
    public final Iterable<ByteBuffer> byteBuffers;
    public final CountDownLatch latch;

    public AddMessage(Iterable<ByteBuffer> byteBuffers) {
        this.byteBuffers = byteBuffers;
        this.latch = new CountDownLatch(1);
    }
}
