package org.davidmoten.eq2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.eq2.event.AddSegment;
import org.davidmoten.eq2.event.EndMessage;
import org.davidmoten.eq2.event.EndWritten;
import org.davidmoten.eq2.event.Event;
import org.davidmoten.eq2.event.MessagePart;
import org.davidmoten.eq2.event.Part;
import org.davidmoten.eq2.event.Written;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;

public class Store extends Completable implements Subscription {

    final LinkedList<Segment> segments = new LinkedList<>();
    final int segmentSize;
    final int chunkSize = 1024 * 1024;
    final Scheduler io;
    final File directory;
    final long addTimeoutMs = TimeUnit.SECONDS.toMillis(1000);

    private final AtomicInteger wip = new AtomicInteger();

    private final SimplePlainQueue<Event> queue = new MpscLinkedQueue<>();
    private RandomAccessFile writeFile;
    private RandomAccessFile writeFileStart;
    private Segment writeSegmentStart;
    private boolean isFirstPart = true;
    private long writePosition;
    private final MessageDigest writeDigest = createDefaultMessageDigest();
    private static final int LENGTH_BYTES = 4;
    private final int minMessageSize = LENGTH_BYTES + writeDigest.getDigestLength() + 4;
    private long messageStartPosition;
    private WriteState writeState = WriteState.SEGMENT_FULL;
    private Subscriber<Part> subscriber;
    private Flowable<Part> source;
    private Subscription sourceSubscription;
    private CompletableObserver child;

    private static enum WriteState {
        SEGMENT_FULL, CREATING_SEGMENT, SEGMENT_READY, WRITING;
    }

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
        this.source = byteBuffers.map(x -> new MessagePart(x));
        return this;
    }

    @Override
    protected void subscribeActual(CompletableObserver child) {
        subscriber = new Subscriber<Part>() {

            @Override
            public void onSubscribe(Subscription s) {
                sourceSubscription = s;
            }

            @Override
            public void onNext(Part part) {
                queue.offer(part);
                drain();
            }

            @Override
            public void onError(Throwable t) {
                child.onError(t);
            }

            @Override
            public void onComplete() {
                child.onComplete();
            }
        };
        this.child = child;
        source.subscribe(subscriber);
        child.onSubscribe(new Disposable() {

            volatile boolean disposed;

            @Override
            public boolean isDisposed() {
                return disposed;
            }

            @Override
            public void dispose() {
                disposed = true;
            }
        });
    }

    @Override
    public void request(long n) {

    }

    @Override
    public void cancel() {

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
        if (event instanceof MessagePart) {
            processEventMessagePart((MessagePart) event);
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

    private void processEventMessagePart(MessagePart event) {
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

    private void writeToSegment(MessagePart event, Segment segment) {
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
                    f.write(event.bb.array(), event.bb.arrayOffset() + event.bb.position(),
                            event.bb.remaining());
                    // watch out because update(ByteBuffer) changes position
                    writeDigest.update(event.bb);
                    long nextWritePosition = segment.start + pos + bbLength + headerBytes;
                    System.out.println("nextWritePosition = " + nextWritePosition);
                    queue.offer(new Written(nextWritePosition));
                } catch (Throwable e) {
                    emitError(e);
                } finally {
                    requestOneMore();
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
        child.onComplete();
    }
    
    private void writeEndMessage(EndMessage event, Segment segment) {
        long pos = writePosition - segment.start;
        System.out.println(
                "end pos=" + pos + ", wp=" + writePosition + ", mstartpos=" + messageStartPosition);
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
                    byte[] checksum = writeDigest.digest();
                    f.write(checksum);
                    // TODO write length 0 after checksum?
                    writeFileStart.seek(messageStartPos - writeSegmentStart.start);
                    System.out.println("Pos=" + pos);
                    int length = (int) (pos - messageStartPos - LENGTH_BYTES);
                    System.out.println("writing length=" + length);
                    writeFileStart.writeInt(length);
                    if (writeFileStart != writeFile) {
                        writeFileStart.close();
                        writeFileStart = null;
                        writeSegmentStart = null;
                    }
                    queue.offer(
                            new EndWritten(pos + segment.start + checksum.length + headerBytes));
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

    private static void createFixedLengthFile(File file, long segmentSize)
            throws FileNotFoundException, IOException {
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.setLength(segmentSize);
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