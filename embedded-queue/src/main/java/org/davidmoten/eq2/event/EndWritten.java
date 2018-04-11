package org.davidmoten.eq2.event;

import org.davidmoten.eq2.event.Event;

public class EndWritten implements Event {

    public final long writePosition;

    public EndWritten(long writePosition) {
        this.writePosition = writePosition;
    }

}
