package org.davidmoten.eq;

import static org.davidmoten.eq.Util.addRequest;
import static org.davidmoten.eq.Util.closeQuietly;
import static org.davidmoten.eq.Util.nextItem;
import static org.davidmoten.eq.Util.prefixWithZeroes;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Scheduler.Worker;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.schedulers.Schedulers;

public final class EmbeddedQueue {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedQueue.class);

    private static final int OFFSET_NUM_BYTES = 8;
    private static final int CHECKSUM_NUM_BYTES = 8;
    private static final int LENGTH_NUM_BYTES = 4;
    private static final int LENGTH_ZERO = 0;
    private static final int EOF = -1;

    private final SimplePlainQueue<Object> queue;
    private final Store store;
    private final AtomicInteger wip = new AtomicInteger();
    private final File directory;
    private final int maxSegmentSize;
    private final long addSegmentMaxWaitTimeMs;
    private final int batchSize;
    private final int messageBufferSize;

    public EmbeddedQueue(File directory, int maxSegmentSize, long addSegmentMaxWaitTimeMs, int batchSize,
            int messageBufferSize) {
        this.directory = directory;
        this.maxSegmentSize = maxSegmentSize;
        this.addSegmentMaxWaitTimeMs = addSegmentMaxWaitTimeMs;
        this.batchSize = batchSize;
        this.queue = new MpscLinkedQueue<>();
        this.store = new Store();
        this.messageBufferSize = messageBufferSize;
    }

    public Reader readSinceTime(long since, OutputStream out) {
        return new Reader(Optional.of(since), Optional.empty(), out, this, batchSize, messageBufferSize);
    }

    public Reader readFromOffset(long offset, OutputStream out) {
        return new Reader(Optional.empty(), Optional.of(offset), out, this, batchSize, messageBufferSize);
    }

    public boolean addMessage(long time, byte[] message) {
        log.info("adding message");
        if (store.writer.segment == null) {
            addAndWaitForSegment();
            log.info("segments={}", store.segments);
        }
        int numBytes = message.length + LENGTH_NUM_BYTES + OFFSET_NUM_BYTES + CHECKSUM_NUM_BYTES;
        store.writer.segment.size += numBytes;
        if (store.writer.segment.size >= maxSegmentSize) {
            store.writer.segment.closeForWrite();
            
            addAndWaitForSegment();
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

    void nextSegment(Reader reader, Segment segment) {
        queue.offer(new RequestNextSegment(reader, segment));
        drain();
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
        return new Segment(file, index);
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
                        r.scheduleRead();
                    } else if (o instanceof Reader) {
                        // handle request
                        ((Reader) o).scheduleRead();
                    } else if (o instanceof RequestAddSegment) {
                        RequestAddSegment add = (RequestAddSegment) o;
                        store.segments.add(add.segment);
                        log.info("added segment {}", add.segment.file.getName());
                        add.latch.countDown();
                    } else if (o instanceof Segment) {
                        // segment has been written to so notify readers
                        for (Reader reader : store.readers) {
                            if (reader.segment == null || o == reader.segment) {
                                reader.scheduleRead();
                            }
                        }
                    } else if (o instanceof RequestNextSegment) {
                        if (store.segments.size() > 0) {
                            log.info("segments = {}",
                                    store.segments.stream().map(x -> x.file.getName()).collect(Collectors.toList()));
                            RequestNextSegment r = (RequestNextSegment) o;
                            Segment nextSegment;
                            if (r.segment == null) {
                                nextSegment = store.segments.get(0);
                            } else {
                                nextSegment = nextItem(store.segments, r.segment);
                            }
                            log.info("setting next segment to " + nextSegment.file);
                            if (nextSegment != null) {
                                r.reader.segment = nextSegment;
                                r.reader.advancingToNextFile = false;
                                r.reader.scheduleRead();
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
        private static final String READ_WRITE = "rw";

        final File file;
        final File index;
        int size;

        RandomAccessFile w;

        Segment(File file, File index) {
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
                log.info("message written to " + file);
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

    }

    public static final class Reader {

        // TODO use since/offset to lookup index
        private final Optional<Long> since;
        private final Optional<Long> offset;
        private final OutputStream out;

        // not final so can set to null for GC and avoid
        private EmbeddedQueue eq;
        private final long batchSize;

        private final AtomicBoolean once = new AtomicBoolean(false);
        private final Worker worker = Schedulers.io().createWorker();
        private final AtomicLong requested = new AtomicLong();
        private final byte[] messageBuffer;

        Segment segment;

        // synchronized by wip
        private RandomAccessFile f;
        boolean advancingToNextFile;
        boolean firstRead = true;

        public Reader(Optional<Long> offset, Optional<Long> since, OutputStream out, EmbeddedQueue eq, long batchSize,
                int messageBufferSize) {
            this.offset = offset;
            this.since = since;
            this.out = out;
            this.eq = eq;
            this.batchSize = batchSize;
            this.messageBuffer = new byte[messageBufferSize];
        }

        void read() {
            log.info("reading");
            if (advancingToNextFile) {
                log.info("abort read because advancing");
                return;
            } else if (segment == null) {
                log.info("segment is null, requesting next");
                eq.nextSegment(this, segment);
                eq.attemptRead(this);
                return;
            } else {
                if (f == null) {
                    log.info("creating RandomAccessFile for {}", segment.file);
                    try {
                        f = new RandomAccessFile(segment.file, "rw");
                        if (firstRead) {
                            long segmentOffset = Long.valueOf(segment.file.getName());
                            if (offset.isPresent()) {
                                f.seek(offset.get() - segmentOffset);
                            } else {
                                // TODO search using time index
                                f.seek(0);
                            }
                            firstRead = false;
                        } else {
                            // start from beginning of segment
                            f.seek(0);
                        }
                    } catch (IOException e) {
                        throw new IORuntimeException(e);
                    }
                }
                if (!read(requested, batchSize, out)) {
                    // there may be more messages waiting (and there are sufficient requests) so try
                    // again but it should be scheduled so that other readers can get a turn to emit
                    eq.attemptRead(this);
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
        boolean read(AtomicLong requested, long batchSize, OutputStream out) {
            if (f == null) {
                return true;
            } else {
                log.info("reading from " + segment.file);
                long r = requested.get();
                long e = 0;
                long attempts = 0;
                boolean result = false;
                try {
                    while (e != r) {
                        long startPosition = f.getFilePointer();
                        int length;
                        synchronized (segment) {
                            length = f.readInt();
                        }
                        if (length == LENGTH_ZERO) {
                            // reset the read
                            f.seek(startPosition);
                            return true;
                        }
                        if (length == EOF) {
                            log.info("advancing to next file");
                            advanceToNextFile();
                            result = false;
                            break;
                        } else if (length + f.getFilePointer() > f.length()) {
                            reportError("message in file " + segment.file + " at position " + startPosition
                                    + " is longer than the length of the file");
                            advanceToNextFile();
                            result = false;
                            break;
                        } else {
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
                            attempts++;
                        }
                        if (attempts == batchSize) {
                            result = true;
                            break;
                        }
                    }
                    Util.addRequest(requested, -e);
                    return result;
                } catch (IOException ex) {
                    cancel();
                    throw new IORuntimeException(ex);
                }
            }
        }

        private void reportError(String message) {
            System.err.println(message);
        }

        private void advanceToNextFile() {
            advancingToNextFile = true;
            Segment seg = segment;
            segment = null;
            closeQuietly(f);
            f = null;
            eq.nextSegment(this, seg);
        }

        void scheduleRead() {
            // TODO ensure that don't schedule too many reads
            // don't want to blow out heap with queued tasks
            // just because writing is happening faster than
            // reading
            worker.schedule(() -> {
                read();
            });
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
            worker.dispose();
            // set to null to help gc of long-lived reference
            eq = null;
        }

        public void start() {
            if (once.compareAndSet(false, true)) {
                eq.addReader(this);
            }
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

        RequestNextSegment(Reader reader, Segment segment) {
            this.segment = segment;
            this.reader = reader;
        }

    }

}
