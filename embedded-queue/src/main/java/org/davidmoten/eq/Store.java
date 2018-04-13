package org.davidmoten.eq;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.davidmoten.eq.event.EndMessage;
import org.davidmoten.eq.event.EndWritten;
import org.davidmoten.eq.event.Event;
import org.davidmoten.eq.event.MessagePart;
import org.davidmoten.eq.event.Part;
import org.davidmoten.eq.event.PartWritten;
import org.davidmoten.eq.event.SegmentCreated;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;

public class Store extends Completable {

    final LinkedList<Segment> segments = new LinkedList<>();
    final int segmentSize;
    final Scheduler io;
    final File directory;
    final long addTimeoutMs = TimeUnit.SECONDS.toMillis(1000);

    private final AtomicInteger wip = new AtomicInteger();

    /**
     * Holds events for serialized processing by {@code drain} method.
     */
    private final SimplePlainQueue<Event> queue = new MpscLinkedQueue<>();

    /**
     * Corresponds to the last segment and is where the next write will occur (not
     * including the rewrite of a length field at the start of a message).
     */
    private RandomAccessFile writeFile;

    /**
     * The start of the message currently being written may be in an another
     * segment. Once the last bytes of the message are written (and its crc32) then
     * the length field (first 4 bytes of the message) are rewritten with the actual
     * value to indicate to a reader that it can consume that message.
     */
    private RandomAccessFile writeFileStart;

    /**
     * The segment corresponding to {@code writeFileStart}.
     */
    private Segment writeSegmentStart;

    /**
     * A message is composed of many ByteBuffer parts and and EndMessage part. If
     * and only if the first part has not been written then {@code isFirstPart} will
     * be true.
     */
    private boolean isFirstPart = true;

    /**
     * The value where the next part will start writing. This is the long value used
     * as a position across all segments. Each segment is named with its start write
     * position.
     */
    private long writePosition;

    /**
     * The position across all segments where the message's first bytes are written.
     */
    private long messageStartPosition;

    /**
     * The current state of the writer.
     */
    private WriteState writeState = WriteState.SEGMENT_FULL;

    private static enum WriteState {
        SEGMENT_FULL, CREATING_SEGMENT, SEGMENT_READY, WRITING;
    }

    private final Checksum writeDigest = new CRC32();
    private static final int CHECKSUM_BYTES = 4;
    private static final int LENGTH_BYTES = 4;
    private final int minMessageSize = LENGTH_BYTES + CHECKSUM_BYTES + 4;

    private Subscriber<Part> subscriber;
    private Flowable<Part> source;
    private Subscription sourceSubscription;
    private CompletableObserver child;

    public Store(File directory, int segmentSize, Scheduler io) {
        this.directory = directory;
        this.segmentSize = segmentSize;
        this.io = io;
    }

    public Completable add(byte[] bytes) {
        return add(ByteBuffer.wrap(bytes));
    }

    public Completable add(ByteBuffer bb) {
        return add(Flowable.just(bb));
    }

    public Completable add(Flowable<ByteBuffer> byteBuffers) {
        this.source = byteBuffers //
                .map(x -> (Part) new MessagePart(x)) //
                .concatWith(Flowable.just(EndMessage.instance()));
        return this;
    }

    @Override
    protected void subscribeActual(CompletableObserver child) {
        subscriber = new Subscriber<Part>() {

            @Override
            public void onSubscribe(Subscription s) {
                sourceSubscription = s;
                s.request(1);
            }

            @Override
            public void onNext(Part part) {
                System.out.println("part=" + part);
                queue.offer(part);
                drain();
            }

            @Override
            public void onError(Throwable t) {
                child.onError(t);
            }

            @Override
            public void onComplete() {
                System.out.println("source complete");
                drain();
            }
        };
        this.child = child;
        child.onSubscribe(new Disposable() {

            volatile boolean disposed;

            @Override
            public boolean isDisposed() {
                return disposed;
            }

            @Override
            public void dispose() {
                disposed = true;
                // TODO write diposal logic
            }
        });
        source.subscribe(subscriber);

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
    }

    private void processEvent(Event event) {
        System.out.println(event);
        if (event instanceof MessagePart) {
            processEventMessagePart((MessagePart) event);
        } else if (event instanceof SegmentCreated) {
            processEventSegmentCreated((SegmentCreated) event);
        } else if (event instanceof PartWritten) {
            processEventWritten((PartWritten) event);
        } else if (event instanceof EndMessage) {
            processEventEndMessage((EndMessage) event);
        } else if (event instanceof EndWritten) {
            processEventEndWritten((EndWritten) event);
        } else {
            throw new RuntimeException("processing not defined for this event: " + event);
        }
    }

    private void processEventSegmentCreated(SegmentCreated event) {
        segments.add(event.segment);
        writeState = WriteState.SEGMENT_READY;
        writeFile = event.file;
        // TODO notify readers?
    }

    private void processEventMessagePart(MessagePart event) {
        if (writeState == WriteState.SEGMENT_FULL) {
            createSegment(event);
        } else if (writeState == WriteState.SEGMENT_READY) {
            writeMessagePart(event, segments.getLast());
        }
        // should not be at WRITING state because is synchronous
    }

    private void processEventEndWritten(EndWritten event) {
        writeState = WriteState.SEGMENT_READY;
        setWritePosition(event.writePosition);
        isFirstPart = true;
        writeFileStart = null;
        writeSegmentStart = null;
    }

    private void setWritePosition(long p) {
        System.out.println("setting writePosition to " + p);
        writePosition = p;
    }

    private void processEventWritten(PartWritten event) {
        writeState = WriteState.SEGMENT_READY;
        setWritePosition(event.writePosition);
    }

    private void createSegment(Event event) {
        writeState = WriteState.CREATING_SEGMENT;
        long pos = writePosition;
        io.scheduleDirect(() -> {
            Segment segment = createSegment(pos);
            queue.offer(new SegmentCreated(segment, segmentSize));
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

    private void writeMessagePart(MessagePart event, Segment segment) {
        // calculate write position relative to segment start
        long pos = writePosition - segment.start;
        if (pos >= segmentSize - minMessageSize) {
            if (pos < segmentSize) {
                // terminate file so read won't be attempted past that point (will move to next
                // segment)
                try {
                    writeFile.seek(pos);
                    writeFile.writeInt(-1);
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
            }
            writeState = WriteState.SEGMENT_FULL;
            queue.offer(event);
        } else {
            boolean firstPart = this.isFirstPart;
            if (firstPart) {
                this.isFirstPart = false;
                messageStartPosition = writePosition;
                writeFileStart = writeFile;
                writeSegmentStart = segment;
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
                    assert event.bb.hasArray();
                    System.out.println("writing '" + new String(event.bb.array(),
                            event.bb.arrayOffset() + event.bb.position(), event.bb.remaining()) + "'");
                    f.write(event.bb.array(), event.bb.arrayOffset() + event.bb.position(), event.bb.remaining());
                    // watch out because update(ByteBuffer) changes position
                    writeDigest.update(event.bb.array(), event.bb.arrayOffset() + event.bb.position(),
                            event.bb.remaining());
                    long nextWritePosition = segment.start + pos + bbLength + headerBytes;
                    System.out.println("nextWritePosition = " + nextWritePosition);
                    queue.offer(new PartWritten(nextWritePosition));
                    requestOneMore();
                } catch (Throwable e) {
                    emitError(e);
                }
                drain();
            });
        }
    }

    private void requestOneMore() {
        sourceSubscription.request(1);
    }

    private void emitError(Throwable e) {
        child.onError(e);
    }

    private void emitComplete() {
        System.out.println("emitting complete");
        child.onComplete();
    }

    private void writeEndMessage(EndMessage event, Segment segment) {
        long pos = writePosition - segment.start;
        System.out.println("end pos=" + pos + ", wp=" + writePosition + ", mstartpos=" + messageStartPosition
                + ", segment=" + segment.file.getName());
        if (pos >= segmentSize - minMessageSize) {
            if (pos < segmentSize) {
                // terminate file so read won't be attempted past that point (will move to next
                // segment)
                try {
                    writeFile.seek(pos);
                    writeFile.writeInt(-1);
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
            }
            writeState = WriteState.SEGMENT_FULL;
            try {
                if (writeFile != writeFileStart) {
                    writeFile.close();
                }
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
                        writeSegmentStart = segment;
                        headerBytes = 4;
                    } else {
                        headerBytes = 0;
                    }
                    int checksum = (int) writeDigest.getValue();
                    f.writeInt(checksum);
                    // TODO write length 0 after checksum?
                    writeFileStart.seek(messageStartPos - writeSegmentStart.start);
                    System.out.println("Pos=" + pos);
                    final int length = (int) (pos + segment.start - messageStartPos - LENGTH_BYTES);
                    System.out.println("writing length=" + length + " at " + writeFileStart.getFilePointer());
                    writeFileStart.writeInt(length);
                    if (writeFileStart != writeFile) {
                        writeFileStart.close();
                        writeFileStart = null;
                        writeSegmentStart = null;
                    }
                    long nextWritePosition = pos + segment.start + CHECKSUM_BYTES + headerBytes;
                    System.out.println("nextWritePosition=" + nextWritePosition);
                    queue.offer(new EndWritten(nextWritePosition));
                    emitComplete();
                } catch (Throwable e) {
                    emitError(e);
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

    private static void createFixedLengthFile(File file, long segmentSize) throws FileNotFoundException, IOException {
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.setLength(segmentSize);
        f.close();
    }

    public int checksumBytes() {
        return CHECKSUM_BYTES;
    }

}