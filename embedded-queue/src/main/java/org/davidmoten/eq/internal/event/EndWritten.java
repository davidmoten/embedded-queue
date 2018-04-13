package org.davidmoten.eq.internal.event;

import org.davidmoten.eq.internal.event.Event;

public class EndWritten implements Event {

    public final long writePosition;

    public EndWritten(long writePosition) {
        this.writePosition = writePosition;
    }

}
