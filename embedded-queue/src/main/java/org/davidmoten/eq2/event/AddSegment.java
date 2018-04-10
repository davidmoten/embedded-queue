package org.davidmoten.eq2.event;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import org.davidmoten.eq2.IORuntimeException;
import org.davidmoten.eq2.Segment;

public class AddSegment implements Event {

    public final Segment segment;
    public final RandomAccessFile file;

    public AddSegment(Segment segment) {
        this.segment = segment;
        try {
            this.file = new RandomAccessFile(segment.file, "rw");
        } catch (FileNotFoundException e) {
            throw new IORuntimeException(e);
        }
    }
    
}
