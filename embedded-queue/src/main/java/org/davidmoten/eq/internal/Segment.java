package org.davidmoten.eq.internal;

import java.io.File;

public class Segment {
     public final File file;
     public final long start;

     public Segment(File file, long start) {
        this.file = file;
        this.start = start;
    }

}
