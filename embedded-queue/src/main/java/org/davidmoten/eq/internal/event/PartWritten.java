package org.davidmoten.eq.internal.event;

public class PartWritten implements Event {
    public final long writePosition;

    public PartWritten(long writePosition) {
        this.writePosition = writePosition;
    }
}