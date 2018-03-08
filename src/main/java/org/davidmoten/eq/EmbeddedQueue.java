package org.davidmoten.eq;

import static org.davidmoten.eq.Util.addRequest;
import static org.davidmoten.eq.Util.closeQuietly;
import static org.davidmoten.eq.Util.prefixWithZeroes;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler.Worker;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.schedulers.Schedulers;

public final class EmbeddedQueue {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedQueue.class);

    private static final int OFFSET_NUM_BYTES = 8;
    private static final int LENGTH_NUM_BYTES = 4;
    private static final int CHECKSUM_NUM_BYTES = 8;
    private static final int LENGTH_ZERO = 0;
    private static final int EOF = -1;

    private final SimplePlainQueue<Object> queue;
    private final Store store;
    private final AtomicInteger wip = new AtomicInteger();
    private final File directory;
    private final int maxSegmentSize;
    private final long addSegmentMaxWaitTimeMs;
    private final int messageBufferSize;

    private EmbeddedQueue(File directory, int maxSegmentSize, long addSegmentMaxWaitTimeMs, int messageBufferSize) {
        this.directory = directory;
        this.maxSegmentSize = maxSegmentSize;
        this.addSegmentMaxWaitTimeMs = addSegmentMaxWaitTimeMs;
        this.queue = new MpscLinkedQueue<>();
        this.store = new Store();
        this.messageBufferSize = messageBufferSize;
    }

    public static Builder directory(File directory) {
        return new Builder(directory);
    }

    public static final class Builder {

        private final File directory;
        private int maxSegmentSize = 1024 * 1024;
        private long addSegmentMaxWaitTimeMs = 30000;
        private int messageBufferSize = 8192;

        Builder(File directory) {
            this.directory = directory;
        }

        public Builder maxSegmentSize(int value) {
            this.maxSegmentSize = value;
            return this;
        }

        public Builder addSegmentMaxWaitTime(long value, TimeUnit unit) {
            this.addSegmentMaxWaitTimeMs = unit.toMillis(value);
            return this;
        }

        public Builder messageBufferSize(int value) {
            this.messageBufferSize = value;
            return this;
        }

        public EmbeddedQueue build() {
            return new EmbeddedQueue(directory, maxSegmentSize, addSegmentMaxWaitTimeMs, messageBufferSize);
        }
    }

    public Reader readSinceTime(long since, OutputStream out) {
        // TODO
        throw new UnsupportedOperationException("not implemented");
    }

    public Reader readFromOffset(long offset, OutputStream out) {
        return new Reader(offset, out, this, messageBufferSize);
    }

    public int inputHeaderLength() {
        return LENGTH_NUM_BYTES + OFFSET_NUM_BYTES + CHECKSUM_NUM_BYTES;
    }

    public boolean addMessage(long time, byte[] message) {
        log.info("adding message");
        if (store.writer.segment == null) {
            addAndWaitForSegment();
            log.info("segments={}", store.segments);
        }
        int numBytes = message.length;
        // want to watch out for the case where a message is bigger than maxSegmentSize
        // and allow for it to be written to a segment
        // TODO add unit test for that situation
        if (store.writer.segment.size > 0) {
            int total = store.writer.segment.size + numBytes;
            if (total > maxSegmentSize) {
                store.writer.segment.closeForWrite();
                addAndWaitForSegment();
            }
        }
        store.writer.segment.size += numBytes;
        if (store.writer.segment.size >= maxSegmentSize) {
            log.info("segments={}", store.segments);
        }
        store.writer.segment.write(time, store.writer.offset, message);
        store.writer.offset += numBytes;
        queue.offer(store.writer.segment);
        drain();
        return true;
    }

    // END OF PUBLIC API

    private void addAndWaitForSegment() {
        RequestAddSegment addSegment = createRequestAddSegment(store.writer.offset);
        drain();
        waitFor(addSegment);
    }

    void attemptRead(Reader reader) {
        queue.offer(reader);
        drain();
    }

    void requestNextSegment(Reader reader, Segment segment, long offset) {
        queue.offer(new RequestNextSegment(reader, segment, offset));
        drain();
    }

    void requestFirstSegment(Reader reader, long offset) {
        requestNextSegment(reader, null, offset);
    }

    private RequestAddSegment createRequestAddSegment(long offset) {
        Segment segment = createSegment(offset);
        store.writer.segment = segment;
        RequestAddSegment addSegment = new RequestAddSegment(segment);
        queue.offer(addSegment);
        return addSegment;
    }

    private Segment createSegment(long offset) {
        String num = prefixWithZeroes(String.valueOf(offset), 16);
        File file = new File(directory, num);
        File index = new File(directory, num + ".idx");
        try {
            file.getParentFile().mkdirs();
            file.createNewFile();
            index.createNewFile();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        return new Segment(offset, file, index);
    }

    void addReader(Reader reader) {
        queue.offer(new RequestAddReader(reader));
        drain();
    }

    void drain() {
        // must be non-blocking
        if (wip.getAndIncrement() == 0) {
            log.info("draining");
            int missed = 1;
            while (true) {
                Object o;
                while ((o = queue.poll()) != null) {
                    log.info("event {}", o);
                    if (o instanceof RequestAddReader) {
                        Reader r = ((RequestAddReader) o).reader;
                        store.add(r);
                        r.read();
                    } else if (o instanceof Reader) {
                        // handle request
                        ((Reader) o).read();
                    } else if (o instanceof RequestAddSegment) {
                        RequestAddSegment add = (RequestAddSegment) o;
                        store.segments.add(add.segment);
                        log.info("added segment {}", add.segment.file.getName());
                        add.latch.countDown();
                        for (Reader reader : store.readers) {
                            reader.segmentAdded();
                        }
                    } else if (o instanceof Segment) {
                        // segment has been written to so notify readers
                        for (Reader reader : store.readers) {
                            if (reader.segment == null || o == reader.segment) {
                                reader.read();
                            }
                        }
                    } else if (o instanceof RequestNextSegment) {
                        RequestNextSegment r = (RequestNextSegment) o;
                        if (r.segment == null) {
                            if (!store.segments.isEmpty()) {
                                // send the first segment (if not past offset then another segment might be
                                // requested by the reader)
                                r.reader.nextSegment(store.segments.get(0));
                            }
                        } else {
                            for (int i = 0; i < store.segments.size(); i++) {
                                Segment seg = store.segments.get(i);
                                if (seg == r.segment) {
                                    if (i < store.segments.size() - 1) {
                                        r.reader.nextSegment(store.segments.get(i + 1));
                                    }
                                    // otherwise no new segment available yet
                                }
                            }
                        }
                    }
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    static final class Store {

        // not synchronized, should only be used by writing thread
        final Writer writer;

        // synchronized by wip
        final List<Reader> readers;

        // synchronized by wip
        final List<Segment> segments;

        Store() {
            this.writer = new Writer();
            this.readers = new ArrayList<>();
            this.segments = new ArrayList<>();
        }

        public void add(Reader reader) {
            readers.add(reader);
        }

    }

    public static final class Writer {
        Segment segment;
        // the overall offset in number of bytes from the start of the first segment to
        // the current write position
        long offset;
    }

    static final class Segment {
        // TODO fixed size segments with messages partitioned across segments when
        // necessary
        private static final String READ_WRITE = "rw";

        final long startOffset;
        final File file;
        final File index;
        int size;

        RandomAccessFile w;

        Segment(long startOffset, File file, File index) {
            this.startOffset = startOffset;
            Preconditions.checkNotNull(file);
            Preconditions.checkNotNull(index);
            this.file = file;
            this.index = index;
        }

        // called only by write thread (singleton)
        void write(long time, long offset, byte[] message) {
            CRC32 crc = new CRC32();
            crc.update(message);
            try {
                if (w == null) {
                    w = new RandomAccessFile(file, READ_WRITE);
                    synchronized (this) {
                        w.writeInt(LENGTH_ZERO);
                    }
                }
                long position = w.getFilePointer();
                w.writeLong(offset);
                w.write(message);
                w.writeLong(crc.getValue());
                w.writeInt(LENGTH_ZERO);
                long position2 = w.getFilePointer();
                w.seek(position - LENGTH_NUM_BYTES);
                synchronized (this) {
                    w.writeInt(message.length);
                }
                // get position ready for next write (just after length bytes)
                w.seek(position2);
                log.info("message written to " + file + " at offset " + (position - LENGTH_NUM_BYTES));
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }

        void closeForWrite() {
            if (w != null) {
                try {
                    w.seek(w.getFilePointer() - LENGTH_NUM_BYTES);
                    w.writeInt(EOF);
                    w.close();
                    w = null;
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
            }
        }

        public long startOffset() {
            return startOffset;
        }

    }

    public static final class Reader {

        // desired starting offset
        private final long offset;
        private final OutputStream out;

        // not final so can set to null for GC and avoid
        private EmbeddedQueue eq;

        private final AtomicBoolean once = new AtomicBoolean(false);
        private final AtomicInteger wip = new AtomicInteger();
        private final Worker worker = Schedulers.io().createWorker();
        private final AtomicLong requested = new AtomicLong();
        private final byte[] messageBuffer;

        private volatile boolean cancelled;

        private enum State {
            WAITING_FIRST_SEGMENT, //
            SEGMENT_ARRIVED_NOT_FIRST, //
            SEGMENT_ARRIVED_FIRST, //
            ADVANCING_TO_NEXT_SEGMENT, //
            REQUESTS_MET, //
            NO_MORE_AVAILABLE, //
            CANCELLED
        }

        Segment segment;

        // should only be accessed by Reader methods
        private State state;

        // synchronized by wip
        private RandomAccessFile f;

        Reader(long offset, OutputStream out, EmbeddedQueue eq, int messageBufferSize) {
            this.offset = offset;
            this.out = out;
            this.eq = eq;
            this.messageBuffer = new byte[messageBufferSize];
            this.state = State.WAITING_FIRST_SEGMENT;
        }

        public void request(long n) {
            if (n <= 0) {
                return; // NOOP
            }
            addRequest(requested, n);

            // indicate that requests exist
            eq.attemptRead(this);
        }

        public void cancel() {
            cancelled = true;
            worker.dispose();
        }

        public void start() {
            if (once.compareAndSet(false, true)) {
                eq.addReader(this);
            }
        }

        // END OF PUBLIC API

        void segmentAdded() {
            worker.schedule(() -> segmentAddedInternal());
        }

        void nextSegment(Segment nextSegment) {
            Preconditions.checkNotNull(nextSegment);
            worker.schedule(() -> nextSegmentInternal(nextSegment));
        }

        void read() {
            // the non-blocking technique using wip is to
            // ensure that we don't schedule too many reads
            // don't want to blow out heap with queued tasks
            // just because writing is happening faster than
            // reading
            if (wip.getAndIncrement() == 0) {
                worker.schedule(() -> {
                    readInternal();
                });
            }
        }

        private void segmentAddedInternal() {
            if (state == State.WAITING_FIRST_SEGMENT) {
                eq.requestFirstSegment(this, offset);
            }
        }

        private void nextSegmentInternal(Segment nextSegment) {
            log.info("nextSegmentInternal, state=" + state + ", file=" + nextSegment.file.getName());
            if (state == State.WAITING_FIRST_SEGMENT) {
                if (nextSegment.startOffset() + nextSegment.file.length() > offset) {
                    segment = nextSegment;
                    state = State.SEGMENT_ARRIVED_FIRST;
                    readInternal();
                } else {
                    eq.requestNextSegment(this, nextSegment, offset);
                }
            } else if (state == State.ADVANCING_TO_NEXT_SEGMENT) {
                // check that is not an old advance result
                if (segment.startOffset < nextSegment.startOffset) {
                    segment = nextSegment;
                    state = State.SEGMENT_ARRIVED_NOT_FIRST;
                    readInternal();
                }
            }
        }

        // access to this method must be serialized
        // as it does io it should be run in an io thread
        private void readInternal() {
            int missed = 1;
            while (true) {
                if (cancelled) {
                    return;
                }
                log.info("reading, state=" + state);
                if (state == State.CANCELLED) {
                    return;
                } else if (state == State.ADVANCING_TO_NEXT_SEGMENT) {
                    return;
                } else if (state == State.WAITING_FIRST_SEGMENT) {
                    return;
                } else {
                    if (state == State.SEGMENT_ARRIVED_FIRST || state == State.SEGMENT_ARRIVED_NOT_FIRST) {
                        try {
                            f = new RandomAccessFile(segment.file, "rw");
                            if (state == State.SEGMENT_ARRIVED_FIRST) {
                                long segmentOffset = Long.valueOf(segment.file.getName());
                                f.seek(offset - segmentOffset);
                            } else {
                                // start from beginning of segment
                                log.info("moving to beginning of file, state=" + state);
                                f.seek(0);
                            }
                        } catch (IOException e) {
                            throw new IORuntimeException(e);
                        }
                    }
                    state = readInternal(requested, out);
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }

        /**
         * Returns true if and only if no more messages were found or if batchSize was
         * reached in terms of attempted emissions to the {@code OutputStream}.
         * Attempted emissions include messages that were determined not to be valid
         * (due to length exceeding available bytes in the file or an unexpected CRC
         * checksum).
         * 
         * @param requested
         * @param batchSize
         * @param out
         * @return
         */
        State readInternal(AtomicLong requested, OutputStream out) {

            log.info("reading from " + segment.file);
            long r = requested.get();
            long e = 0;
            State result = null;
            try {
                while (true) {
                    if (e == r) {
                        result = State.REQUESTS_MET;
                        break;
                    }
                    long startPosition = f.getFilePointer();
                    int length;
                    synchronized (segment) {
                        length = f.readInt();
                    }
                    if (length == LENGTH_ZERO) {
                        // reset the read
                        f.seek(startPosition);
                        result = State.NO_MORE_AVAILABLE;
                        break;
                    }
                    if (length == EOF) {
                        log.info("advancing to next segment");
                        result = State.ADVANCING_TO_NEXT_SEGMENT;
                        advanceToNextFile();
                        break;
                    } else if (length + OFFSET_NUM_BYTES - LENGTH_NUM_BYTES + f.getFilePointer() > f.length()) {
                        reportError("message in file " + segment.file + " at position " + startPosition
                                + " is longer than the length of the file");
                        result = State.ADVANCING_TO_NEXT_SEGMENT;
                        advanceToNextFile();
                        break;
                    } else {
                        // read position is just after length field
                        final byte[] message;
                        if (messageBuffer.length >= length) {
                            message = messageBuffer;
                        } else {
                            message = new byte[length];
                        }
                        long offset = f.readLong();
                        f.readFully(message, 0, length);
                        long expected = f.readLong();
                        CRC32 crc = new CRC32();
                        crc.update(message, 0, length);
                        if (crc.getValue() != expected) {
                            reportError("crc doesn't match for message in file " + segment.file + " at position "
                                    + startPosition);
                        } else {
                            out.write(Util.toBytes(length));
                            out.write(Util.toBytes(offset));
                            out.write(message, 0, length);
                            e++;
                            log.info("{} bytes written to output={}", length, new String(message, 0, length));
                        }
                    }
                }
                Util.addRequest(requested, -e);
                return result;
            } catch (IOException ex) {
                cancel();
                throw new IORuntimeException(ex);
            }
        }

        private void reportError(String message) {
            System.err.println(message);
        }

        private void advanceToNextFile() {
            Segment seg = segment;
            closeQuietly(f);
            f = null;
            eq.requestNextSegment(this, seg, offset);
        }

    }

    private void waitFor(RequestAddSegment addSegment) {
        try {
            boolean added = addSegment.latch.await(addSegmentMaxWaitTimeMs, TimeUnit.MILLISECONDS);
            if (!added) {
                throw new RuntimeException("could not create new segment");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Request classes

    static final class RequestAddReader {

        private final Reader reader;

        RequestAddReader(Reader reader) {
            this.reader = reader;
        }
    }

    static final class RequestAddSegment {
        final Segment segment;
        final CountDownLatch latch = new CountDownLatch(1);

        RequestAddSegment(Segment segment) {
            this.segment = segment;
        }
    }

    static final class RequestNextSegment {
        final Segment segment;
        final Reader reader;
        final long offset;

        RequestNextSegment(Reader reader, Segment segment, long offset) {
            this.segment = segment;
            this.reader = reader;
            this.offset = offset;
        }

    }

}
