package org.davidmoten.eq.internal.event;

public class SegmentFull implements Event{
    
    public final MessagePart messagePart;

    public SegmentFull(MessagePart event) {
        this.messagePart = event;        
    }
}
