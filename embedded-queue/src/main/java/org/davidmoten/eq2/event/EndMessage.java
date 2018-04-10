package org.davidmoten.eq2.event;

import java.util.concurrent.CountDownLatch;

import org.davidmoten.eq2.event.Event;

public class EndMessage implements Event {

    public final CountDownLatch latch;

    public EndMessage() {
        latch = new CountDownLatch(1);
    }

}
