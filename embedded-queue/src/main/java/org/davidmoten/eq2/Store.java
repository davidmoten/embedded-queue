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
import org.davidmoten.eq2.event.EndWritten;
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
    final long addTimeoutMs = TimeUnit.SECONDS.toMillis(1000);

    private final AtomicInteger wip = new AtomicInteger();

    private final SimplePlainQueue<Event> queue = new MpscLinkedQueue<>();
    private RandomAccessFile writeFile;
    private RandomAccessFile writeFileStart;
    private boolean isFirstPart = true;
    private long writePosition;
    private final MessageDigest writeDigest = createDefaultMessageDigest();
    private static final int LENGTH_BYTES = 4;
    private final int minMessageSize = LENGTH_BYTES + writeDigest.getDigestLength() + 4;
    private long messageStartPosition;

    private static enum WriteState {
        SEGMENT_FULL, CREATING_SEGMENT, SEGMENT_READY, WRITING;
    }

    private WriteState writeState = WriteState.SEGMENT_FULL;

    public Store(File directory, int segmentSize) {
        this.directory = directory;
        this.segmentSize = segmentSize;
    }

    public boolean add(byte[] bytes) {
        return add(ByteBuffer.wrap(bytes));
    }

    public boolean add(ByteBuffer bb) {
        if (addPart(bb)) {
            return endMessage();
        } else {
            return false;
        }
    }

    public boolean addPart(byte[] bytes) {
        return addPart(ByteBuffer.wrap(bytes));
    }

    public boolean addPart(ByteBuffer bb) {
        MesagePart add = new MesagePart(bb);
        queue.offer(add);
        drain();
        try {
            return add.latch.await(addTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
        System.out.println(event);
        if (event instanceof MesagePart) {
            processEventMessagePart((MesagePart) event);
        } else if (event instanceof AddSegment) {
            processEventAddSegment((AddSegment) event);
        } else if (event instanceof Written) {
            processEventWritten((Written) event);
        } else if (event instanceof EndMessage) {
            processEventEndMessage((EndMessage) event);
        } else if (event instanceof EndWritten) {
            processEventEndWritten((EndWritten) event);
        } else {
            throw new RuntimeException("processing not defined for this event: " + event);
        }
    }

    private void processEventAddSegment(AddSegment event) {
        segments.add(event.segment);
        writeState = WriteState.SEGMENT_READY;
        writeFile = event.file;
        // TODO notify readers?
    }

    private void processEventMessagePart(MesagePart event) {
        if (writeState == WriteState.SEGMENT_FULL) {
            createSegment(event);
        } else if (writeState == WriteState.SEGMENT_READY) {
            writeToSegment(event, segments.getLast());
        }
        // should not be at WRITING state because is synchronous
    }

    private void processEventEndWritten(EndWritten event) {
        writeState = WriteState.SEGMENT_READY;
        setWritePosition(event.writePosition);
        isFirstPart = true;
    }

    private void setWritePosition(long p) {
        System.out.println("setting writePosition to " + p);
        writePosition = p;
    }

    private void processEventWritten(Written event) {
        writeState = WriteState.SEGMENT_READY;
        setWritePosition(event.writePosition);
    }

    private void createSegment(Event event) {
        writeState = WriteState.CREATING_SEGMENT;
        long pos = writePosition;
        io.scheduleDirect(() -> {
            Segment segment = createSegment(pos);
            queue.offer(new AddSegment(segment));
            // retry with add event
            queue.offer(event);
            drain();
        });
    }

    private void processEventEndMessage(EndMessage event) {
        System.out.println(writeState);
        if (writeState == WriteState.SEGMENT_FULL) {
            createSegment(event);
        } else if (writeState == WriteState.SEGMENT_READY) {
            writeEndMessage(event, segments.getLast());
        }
    }

    private void writeToSegment(MesagePart event, Segment segment) {
        // calculate write position relative to segment start
        long pos = writePosition - segment.start;
        if (pos >= segmentSize - minMessageSize) {
            writeState = WriteState.SEGMENT_FULL;
            queue.offer(event);
        } else {
            boolean firstPart = this.isFirstPart;
            if (firstPart) {
                this.isFirstPart = false;
                messageStartPosition = writePosition;
                writeFileStart = writeFile;
                System.out.println("messageStartPosition=" + messageStartPosition);
            }
            writeState = WriteState.WRITING;
            io.scheduleDirect(() -> {
                try {
                    RandomAccessFile f = writeFile;
                    f.seek(pos);
                    final int headerBytes;
                    if (firstPart) {
                        // set length to zero until last part written
                        f.writeInt(0);
                        headerBytes = 4;
                    } else {
                        headerBytes = 0;
                    }
                    int bbLength = event.bb.remaining();
                    f.write(event.bb.array(), event.bb.arrayOffset() + event.bb.position(),
                            event.bb.remaining());
                    // watch out because update(ByteBuffer) changes position
                    writeDigest.update(event.bb);
                    long nextWritePosition = segment.start + pos + bbLength + headerBytes;
                    System.out.println("nextWritePosition = " + nextWritePosition);
                    queue.offer(new Written(nextWritePosition));
                    event.latch.countDown();
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
                drain();
            });
        }
    }

    private void writeEndMessage(EndMessage event, Segment segment) {
        long pos = writePosition - segment.start;
        System.out.println(
                "end pos=" + pos + ", wp=" + writePosition + ", mstartpos=" + messageStartPosition);
        if (pos >= segmentSize - minMessageSize) {
            writeState = WriteState.SEGMENT_FULL;
            try {
                writeFile.close();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
            queue.offer(event);
        } else {
            boolean firstPart = this.isFirstPart;
            if (firstPart) {
                this.isFirstPart = false;
                messageStartPosition = writePosition;
            }
            long messageStartPos = messageStartPosition;
            writeState = WriteState.WRITING;
            io.scheduleDirect(() -> {
                try {
                    RandomAccessFile f = writeFile;
                    f.seek(pos);
                    final int headerBytes;
                    if (firstPart) {
                        // set length to zero until last part written
                        f.writeInt(0);
                        writeFileStart = writeFile;
                        headerBytes = 4;
                    } else {
                        headerBytes = 0;
                    }
                    byte[] checksum = writeDigest.digest();
                    f.write(checksum);
                    // TODO write length 0 after checksum?
                    writeFileStart.seek(messageStartPos);
                    System.out.println("Pos=" + pos);
                    int length = (int) (pos - messageStartPos - LENGTH_BYTES);
                    writeFileStart.writeInt(length);
                    if (writeFileStart!= writeFile) {
                        writeFileStart.close();
                        writeFileStart = null;
                    }
                    queue.offer(new EndWritten(pos + checksum.length + headerBytes));
                    event.latch.countDown();
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