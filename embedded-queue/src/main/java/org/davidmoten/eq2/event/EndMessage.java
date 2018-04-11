package org.davidmoten.eq2.event;

import java.util.concurrent.CountDownLatch;

public class EndMessage implements Event {

    public final CountDownLatch latch;

    public EndMessage() {
        latch = new CountDownLatch(1);
    }

}
