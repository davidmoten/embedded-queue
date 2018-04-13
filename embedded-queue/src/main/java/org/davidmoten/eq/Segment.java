package org.davidmoten.eq;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Segment {
     public final File file;
     public final long start;

     public Segment(File file, long start) {
        this.file = file;
        this.start = start;
    }

}
