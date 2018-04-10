package org.davidmoten.eq2.event;

import org.davidmoten.eq2.Segment;

public class AddSegment implements Event {

    public final Segment segment;

    public AddSegment(Segment segment) {
        this.segment = segment;
    }
    
}
