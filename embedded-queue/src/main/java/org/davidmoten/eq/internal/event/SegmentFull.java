package org.davidmoten.eq.internal.event;

public class SegmentFull implements Event{
    
    public final Event event;

    public SegmentFull(Event event) {
        this.event = event;        
    }
}
