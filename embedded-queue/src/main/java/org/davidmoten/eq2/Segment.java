package org.davidmoten.eq2;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Segment {
     File file;
     long start;
     List<Fragment> fragments;
     
     public static void main(String[] args) throws NoSuchAlgorithmException {
        String s= "the quick brown fox jumped over the lazy dog";
        MessageDigest d = MessageDigest.getInstance("MD5");
        d.update(s.getBytes());
        System.out.println(d.digest().length);
    }
}
