package org.davidmoten.eq2.event;

public class Written implements Event {
    public final long writePosition;

    public Written(long writePosition) {
        this.writePosition = writePosition;
    }
}
