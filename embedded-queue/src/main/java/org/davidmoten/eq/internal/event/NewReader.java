package org.davidmoten.eq.internal.event;

import java.nio.ByteBuffer;

import org.davidmoten.eq.internal.event.Event;
import org.reactivestreams.Subscriber;

public class NewReader implements Event {

    public NewReader(Subscriber<? super ByteBuffer> subscriber, long positionGlobal) {
        // TODO Auto-generated constructor stub
    }

}
