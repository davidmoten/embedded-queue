package org.davidmoten.eq2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.eq2.event.AddSegment;
import org.davidmoten.eq2.event.EndMessage;
import org.davidmoten.eq2.event.Event;
import org.davidmoten.eq2.event.MesagePart;
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
        return add(ByteBuffer.wrap(bytes));
    }

    public boolean add(ByteBuffer bb) {
        addPart(bb);
        return endMessage();
    }

    public void addPart(byte[] bytes) {
        addPart(ByteBuffer.wrap(bytes));
    }

    public void addPart(ByteBuffer bb) {
        MesagePart add = new MesagePart(bb);
        queue.offer(add);
        drain();
    }

    public boolean endMessage() {
        EndMessage m = new EndMessage();
        queue.offer(m);
        drain();
        try {
            return m.latch.await(addTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private final AtomicInteger wip = new AtomicInteger();

    private final SimplePlainQueue<Event> queue = new MpscLinkedQueue<>();
    private RandomAccessFile writeFile;
    private long writePositionPart;
    private boolean isFirstPart = true;

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
        if (event instanceof MesagePart) {
            processEventMessagePart((MesagePart) event);
        } else if (event instanceof AddSegment) {
            processEventAddSegment((AddSegment) event);
        } else if (event instanceof Written) {
            processEventWritten((Written) event);
        } else if (event instanceof EndMessage) {
            processEventEndMessage((EndMessage) event);
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

    private void processEventMessagePart(MesagePart event) {
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

    private void processEventEndMessage(EndMessage event) {
        // TODO Auto-generated method stub
    }

    private final MessageDigest writeDigest = createDefaultMessageDigest();

    private static final int LENGTH_BYTES = 4;
    private final int minMessageSize = LENGTH_BYTES + writeDigest.getDigestLength() + 4;

    private void writeToSegment(MesagePart event, Segment segment) {
        // calculate write position relative to segment start
        long pos = writePosition - segment.start;
        if (pos >= segmentSize - minMessageSize) {
            writeState = WriteState.SEGMENT_FULL;
            queue.offer(event);
        } else {
            writePositionPart = writePosition;
            boolean firstPart = this.isFirstPart;
            this.isFirstPart = false;
            writeState = WriteState.WRITING;
            io.scheduleDirect(() -> {
                try {
                    RandomAccessFile f = writeFile;
                    f.seek(pos);
                    if (firstPart) {
                        // set length to zero until last part written
                        f.writeInt(0);
                    }
                    f.write(event.bb.array(), event.bb.position(), event.bb.limit());
                    writeDigest.update(event.bb);
                    queue.offer(new Written(pos + 1));
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
                drain();
            });
        }
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