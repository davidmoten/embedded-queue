package org.davidmoten.eq2;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedList;

public class Store {

    final LinkedList<Segment> segments = new LinkedList<>();
    final int segmentSize = 10 * 1024 * 1024;
    final int chunkSize = 1024 * 1024;
    Segment writeSegment;
    long writePosition;
    final MessageDigest digest = createMessageDigest();

    public void add(byte[] bytes) {
        add(Collections.singletonList(ByteBuffer.wrap(bytes)));
    }

    // Iterable is a reasonable choice (as opposed to Flowable) because this method
    // is synchronous to ensure that client knows message has been persisted to the
    // queue
    public void add(Iterable<ByteBuffer> byteBuffers) {
    }

    private static MessageDigest createMessageDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}