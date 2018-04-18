package org.davidmoten.eq.internal.event;

public class SegmentFull implements Event {

    public final Part part;

    public SegmentFull(Part part) {
        this.part = part;
    }
}
