package org.davidmoten.eq2.event;

import org.davidmoten.eq2.event.Event;

public class WrittenEnd implements Event {

    public final long writePosition;

    public WrittenEnd(long writePosition) {
        this.writePosition = writePosition;
    }

}
