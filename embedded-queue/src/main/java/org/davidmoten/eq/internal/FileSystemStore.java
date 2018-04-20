package org.davidmoten.eq.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.eq.IORuntimeException;
import org.davidmoten.eq.Store;
import org.davidmoten.eq.internal.event.BatchFinished;
import org.davidmoten.eq.internal.event.CancelReader;
import org.davidmoten.eq.internal.event.EndWritten;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessageEnd;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.Part;
import org.davidmoten.eq.internal.event.PartWritten;
import org.davidmoten.eq.internal.event.RequestBatch;
import org.davidmoten.eq.internal.event.SegmentCreated;
import org.davidmoten.eq.internal.event.SegmentFull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;

public final class FileSystemStore extends Completable implements Store, StoreWriter, StoreReader {

    final LinkedList<Segment> segments = new LinkedList<>();
    final int segmentSize;
    final Scheduler io;
    final File directory;
    private final WriteHandler writeHandler;
    final ReadHandler readHandler;

    private final AtomicInteger wip = new AtomicInteger();

    /**
     * Holds events for serialized processing by {@code drain} method.
     */
    private final SimplePlainQueue<Event> queue = new MpscLinkedQueue<>();

    private Subscriber<Part> subscriber;
    private Flowable<Part> source;
    private Subscription sourceSubscription;
    private CompletableObserver child;
    private final Map<Reader, ReaderState> readers = new HashMap<>();
    private final Set<Reader> reading = new HashSet<>();
    

    public FileSystemStore(File directory, int segmentSize, Scheduler io) {
        this.directory = directory;
        this.segmentSize = segmentSize;
        this.io = io;
        this.writeHandler = new WriteHandler(this, segmentSize, io);
        this.readHandler = new ReadHandler(this, io);
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
                .concatWith(Flowable.just(MessageEnd.instance()));
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
        if (event instanceof SegmentFull) {
            writeHandler.handleSegmentFull((SegmentFull) event);
        } else if (event instanceof SegmentCreated) {
            writeHandler.handleSegmentCreated((SegmentCreated) event);
        } else if (event instanceof Part) {
            writeHandler.handlePart((MessagePart) event);
        } else if (event instanceof PartWritten) {
            sourceSubscription.request(1);
        } else if (event instanceof EndWritten) {
            emitComplete();
        } else if (event instanceof RequestBatch) {
            readHandler.handleRequestBatch((RequestBatch) event);
        } else if (event instanceof BatchFinished) {
            reading.remove(((BatchFinished) event).reader);
        }
    }

    @Override
    public void errorOccurred(Throwable e) {
        child.onError(e);
    }

    private void emitComplete() {
        System.out.println("emitting complete");
        child.onComplete();
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

    @Override
    public void send(Event event) {
        queue.offer(event);
        drain();
    }

    @Override
    public void send(Event event1, Event event2) {
        queue.offer(event1);
        queue.offer(event2);
        drain();
    }

    @Override
    public Segment writeSegment() {
        return segments.get(segments.size() - 1);
    }

    @Override
    public void writeInt(int positionLocal, int value) {
        RandomAccessFile f = writeSegment().writeFile();
        try {
            f.seek(positionLocal);
            f.writeInt(value);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    @Override
    public void writeByte(int positionLocal, int value) {
        RandomAccessFile f = writeSegment().writeFile();
        try {
            f.seek(positionLocal);
            f.write(value);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

    }

    @Override
    public void write(int positionLocal, ByteBuffer bb, int length) {
        RandomAccessFile f = writeSegment().writeFile();
        try {
            f.seek(positionLocal);
            if (bb.hasArray()) {
                f.write(bb.array(), bb.arrayOffset() + bb.position(), length);
            } else {
                for (int i = 0; i < length; i++) {
                    f.write(bb.get());
                }
            }
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

    }

    @Override
    public void writeInt(Segment segment, int positionLocal, int value) {
        RandomAccessFile f = writeSegment().writeFile();
        try {
            f.seek(positionLocal);
            f.writeInt(value);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

    }

    @Override
    public Segment createSegment(long positionGlobal) {
        return new Segment(nextFile(positionGlobal), positionGlobal);
    }

    @Override
    public void addSegment(Segment segment) {
        segments.add(segment);
    }

    @Override
    public void closeForWrite(Segment segment) {
        segment.closeForWrite();
    }

    @Override
    public Flowable<Flowable<ByteBuffer>> read(long positionGlobal) {
        return group(new FlowableRead(this, positionGlobal));
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    // TODO unit test
    static Flowable<Flowable<ByteBuffer>> group(FlowableRead f) {
        return Flowable.defer(() -> {
            long[] count = new long[1];
            return f.groupBy(x -> count[0]) //
                    .map(g -> (Flowable<ByteBuffer>) (Flowable<?>) g.takeWhile(x -> x instanceof ByteBuffer)
                            .doOnComplete(() -> count[0]++));
        });
    }

    @Override
    public void requestBatch(Reader reader) {
        queue.offer(new RequestBatch(reader));
        drain();

    }

    @Override
    public void cancel(Reader reader) {
        queue.offer(new CancelReader(reader));
        drain();
    }

    public static final class ReaderState {

        public final Reader reader;
        // mutable
        public long readPositionGlobal;

        public ReaderState(Reader reader) {
            this.reader = reader;
            this.readPositionGlobal = reader.startPositionGlobal();
        }
    }

    @Override
    public ReaderState state(Reader reader) {
        if (!readers.containsKey(reader)) {
            readers.put(reader,  new ReaderState(reader));
        }
        return readers.get(reader);
    }

    @Override
    public Optional<Segment> segment(long positionGlobal) {
        for (Segment segment : segments) {
            if (positionGlobal < segment.start + segmentSize) {
                return Optional.of(segment);
            }
        }
        return Optional.empty();
    }

}