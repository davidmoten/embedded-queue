package org.davidmoten.eq.internal.event;

import org.davidmoten.eq.AbstractStore.Info;

public class SegmentFull implements Event{
    
    public final Event event;

    public SegmentFull(Event event, Info info) {
        this.event = event;        
    }
}
