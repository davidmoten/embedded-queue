package org.davidmoten.eq.event;

import org.davidmoten.eq.event.Event;

public class EndWritten implements Event {

    public final long writePosition;

    public EndWritten(long writePosition) {
        this.writePosition = writePosition;
    }

}
