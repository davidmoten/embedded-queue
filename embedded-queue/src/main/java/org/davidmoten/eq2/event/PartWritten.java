package org.davidmoten.eq2.event;

public class PartWritten implements Event {
    public final long writePosition;

    public PartWritten(long writePosition) {
        this.writePosition = writePosition;
    }
}
