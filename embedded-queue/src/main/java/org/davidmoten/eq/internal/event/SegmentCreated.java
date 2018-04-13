package org.davidmoten.eq.internal.event;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.davidmoten.eq.IORuntimeException;
import org.davidmoten.eq.internal.Segment;

public class SegmentCreated implements Event {

    public final Segment segment;
    public final RandomAccessFile file;

    public SegmentCreated(Segment segment, long length) {
        this.segment = segment;
        try {
            this.file = new RandomAccessFile(segment.file, "rw");
            file.setLength(length);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

}
