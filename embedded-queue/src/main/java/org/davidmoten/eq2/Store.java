package org.davidmoten.eq2;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.eq2.message.Add;
import org.davidmoten.eq2.message.Event;

import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;

public class Store {

    final LinkedList<Segment> segments = new LinkedList<>();
    final int segmentSize = 10 * 1024 * 1024;
    final int chunkSize = 1024 * 1024;
    final MessageDigest digest = createDefaultMessageDigest();

    Segment writeSegment;
    long writePosition;

    public void add(byte[] bytes) {
        add(Collections.singletonList(ByteBuffer.wrap(bytes)));
    }

    // Iterable is a reasonable choice (as opposed to Flowable) because this method
    // is synchronous to ensure that client knows message has been persisted to the
    // queue
    public void add(Iterable<ByteBuffer> byteBuffers) {

    }

    private final AtomicInteger wip = new AtomicInteger();

    private final SimplePlainQueue<Event> queue = new SpscLinkedArrayQueue<>(16);

    private volatile boolean cancelled;

    private void drain() {
        if (wip.getAndIncrement() != 0) {
            int missed = 1;
            while (true) {
                Event m = queue.poll();
                if (m != null) {
                    processEvent(m);
                } else {
                    break;
                }
            }
            missed = wip.addAndGet(-missed);
            if (missed == 0) {
                return;
            }
        }

    }

    private void processEvent(Event event) {
        if (event instanceof Add) {
            processEventAdd((Add) event);
        } else {
            throw new RuntimeException("processing not defined for this event: "+ event);
        }
    }

    private void processEventAdd(Add event) {
        
    }

    private static MessageDigest createDefaultMessageDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}