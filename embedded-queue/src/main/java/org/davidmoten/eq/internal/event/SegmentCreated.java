package org.davidmoten.eq.internal.event;

import org.davidmoten.eq.internal.Segment;

public class SegmentCreated implements Event {

    public final Segment segment;

    public SegmentCreated(Segment segment, long length) {
        this.segment = segment;
        segment.createFile(length);
    }

}
