package org.davidmoten.eq2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.eq2.message.Add;
import org.davidmoten.eq2.message.Event;

import io.reactivex.Scheduler;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.schedulers.Schedulers;

public class Store {

    final LinkedList<Segment> segments = new LinkedList<>();
    final int segmentSize = 10 * 1024 * 1024;
    final int chunkSize = 1024 * 1024;
    final MessageDigest digest = createDefaultMessageDigest();
    final Scheduler io = Schedulers.from(Executors.newFixedThreadPool(10));
    final File directory = new File("target");
    final long addTimeoutMs = TimeUnit.SECONDS.toMillis(5);

    Segment writeSegment;
    long writePosition;

    public boolean add(byte[] bytes) {
        return add(Collections.singletonList(ByteBuffer.wrap(bytes)));
    }

    // Iterable is a reasonable choice (as opposed to Flowable) because this method
    // is synchronous to ensure that client knows message has been persisted to the
    // queue

    /**
     * Returns true if message has been persisted to queue. Returns false due to a
     * timeout in which case a retry may be advisable.
     * 
     * @param byteBuffers
     *            message bytes
     * @return true if message has been persisted to queue. Returns false due to a
     *         timeout in which case a retry may be advisable
     */
    public boolean add(Iterable<ByteBuffer> byteBuffers) {
        Add add = new Add(byteBuffers);
        queue.offer(add);
        try {
            return add.latch.await(addTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private final AtomicInteger wip = new AtomicInteger();

    private final SimplePlainQueue<Event> queue = new MpscLinkedQueue<>();

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
            throw new RuntimeException("processing not defined for this event: " + event);
        }
    }

    private static enum WriteState {
        NO_SEGMENTS, SEGMENT_FULL, CREATING_SEGMENT, SEGMENT_NOT_FULL;
    }

    private WriteState writeState = WriteState.SEGMENT_FULL;

    private void processEventAdd(Add event) {
        if (writeState == WriteState.NO_SEGMENTS) {
            writeState = WriteState.CREATING_SEGMENT;
            io.scheduleDirect(() -> {
                createSegment();
            });
        }
    }

    private void createSegment() {
        segments.add(new Segment(nextFile(writePosition), writePosition));
    }

    private File nextFile(long writePosition) {
        File file = new File(directory, writePosition + "");
        try {
            file.createNewFile();
            // write the complete file now
            // if file already exists that's ok
            // we will overwrite it by setting the
            // first byte to zero
            RandomAccessFile f = new RandomAccessFile(file, "rw");
            f.seek(segmentSize - 1);
            f.write(0);
            f.seek(0);
            f.write(0);
            f.close();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        return file;
    }

    private static MessageDigest createDefaultMessageDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}