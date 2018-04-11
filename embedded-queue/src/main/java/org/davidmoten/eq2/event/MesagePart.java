package org.davidmoten.eq2.event;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class MesagePart implements Event {
    
    public final ByteBuffer bb;
    public final CountDownLatch latch;

    public MesagePart(ByteBuffer bb) {
        this.bb = bb;
        this.latch = new CountDownLatch(1);
    }
}
