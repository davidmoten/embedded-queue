package org.davidmoten.eq2;

import java.io.File;
import java.io.FileNotFoundException;
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

import org.davidmoten.eq2.event.AddMessage;
import org.davidmoten.eq2.event.AddSegment;
import org.davidmoten.eq2.event.Event;
import org.davidmoten.eq2.event.Written;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.schedulers.Schedulers;

public class Store {

    final LinkedList<Segment> segments = new LinkedList<>();
    final int segmentSize;
    final int chunkSize = 1024 * 1024;
    final MessageDigest digest = createDefaultMessageDigest();
    final Scheduler io = Schedulers.from(Executors.newFixedThreadPool(10));
    final File directory;
    final long addTimeoutMs = TimeUnit.SECONDS.toMillis(1);

    private static enum WriteState {
        SEGMENT_FULL, CREATING_SEGMENT, SEGMENT_READY, WRITING;
    }

    private WriteState writeState = WriteState.SEGMENT_FULL;

    public Store(File directory, int segmentSize) {
        this.directory = directory;
        this.segmentSize = segmentSize;
    }

    Segment writeSegment;
    long writePosition;

    public boolean add(byte[] bytes) {
        return add(Collections.singletonList(ByteBuffer.wrap(bytes)));
    }

    /**
     * Returns true if message has been persisted to queue. Returns false due to a
     * timeout in which case a retry may be advisable.
     * 
     * @param byteBuffers
     *            message bytes
     * @return true if message has been persisted to queue. Returns false due to a
     *         timeout in which case a retry may be advisable
     */
    // Iterable is a reasonable choice (as opposed to Flowable) because this method
    // is synchronous to ensure that client knows message has been persisted to the
    // queue
    public boolean add(Iterable<ByteBuffer> byteBuffers) {
        AddMessage add = new AddMessage(byteBuffers);
        queue.offer(add);
        drain();
        try {
            return add.latch.await(addTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private final AtomicInteger wip = new AtomicInteger();

    private final SimplePlainQueue<Event> queue = new MpscLinkedQueue<>();
    private RandomAccessFile writeFile;

    private void drain() {
        // this method is non-blocking
        // so that any call to drain should
        // rocket through (albeit performing
        // volatile reads and writes). No IO
        // should be done by this drain call (
        // scheduled IO work is ok),
        if (wip.getAndIncrement() == 0) {
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
        if (event instanceof AddMessage) {
            processEventAddMessage((AddMessage) event);
        } else if (event instanceof AddSegment) {
            processEventAddSegment((AddSegment) event);
        } else if (event instanceof Written) {
            processEventWritten((Written) event);
        } else {
            throw new RuntimeException("processing not defined for this event: " + event);
        }
    }

    private void processEventWritten(Written event) {
        writeState = WriteState.SEGMENT_READY;
        writePosition = event.writePosition;
    }

    private void processEventAddSegment(AddSegment event) {
        segments.add(event.segment);
        writeState = WriteState.SEGMENT_READY;
        writeSegment = event.segment;
        writeFile = event.file;
        // TODO notify readers?
    }

    private void processEventAddMessage(AddMessage event) {
        if (writeState == WriteState.SEGMENT_FULL) {
            writeState = WriteState.CREATING_SEGMENT;
            long pos = writePosition;
            io.scheduleDirect(() -> {
                Segment segment = createSegment(pos);
                queue.offer(new AddSegment(segment));
                // retry with add event
                queue.offer(event);
                drain();
            });
        } else if (writeState == WriteState.SEGMENT_READY) {
            writeToSegment(event, segments.getLast());
        }
        // should not be at WRITING state because is synchronous
    }

    private void writeToSegment(AddMessage event, Segment segment) {
        // calculate write position relative to segment start
        long pos = writePosition - segment.start;
        writeState = WriteState.WRITING;
        io.scheduleDirect(() -> {
            try {
                RandomAccessFile f = writeFile;
                f.seek(pos);
                f.write(0);
                event.latch.countDown();
                queue.offer(new Written(pos + 1));
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
            drain();
        });
    }

    private Segment createSegment(long pos) {
        return new Segment(nextFile(pos), pos);
    }

    private File nextFile(long writePosition) {
        File file = new File(directory, writePosition + "");
        try {
            Preconditions.checkArgument(!file.exists());
            file.createNewFile();
            createFixedLengthFile(file, segmentSize);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        return file;
    }

    private static void createFixedLengthFile(File file, long segmentSize)
            throws FileNotFoundException, IOException {
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek(segmentSize - 1);
        f.write(0);
        f.close();
    }

    private static MessageDigest createDefaultMessageDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}