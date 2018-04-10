package org.davidmoten.eq2;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Segment {
     final File file;
     final long start;

     public Segment(File file, long start) {
        this.file = file;
        this.start = start;
    }

}
