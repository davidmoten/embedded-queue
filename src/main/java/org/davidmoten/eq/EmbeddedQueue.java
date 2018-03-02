package org.davidmoten.eq;

import static org.davidmoten.eq.Util.addRequest;
import static org.davidmoten.eq.Util.prefixWithZeroes;

import java.io.File;
import java.io.FileNotFoundException;
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

import io.reactivex.Scheduler.Worker;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.schedulers.Schedulers;

public final class EmbeddedQueue {

    private static final int CHECKSUM_NUM_BYTES = 8;
    private static final int LENGTH_NUM_BYTES = 4;
    private final SimplePlainQueue<Object> queue;
    private final Store store;
    private final AtomicInteger wip = new AtomicInteger();
    private final File directory;
    private final int maxSegmentSize;
    private final long addSegmentMaxWaitTimeMs;

    // only used by write thread
    private int fileNumber;

    public EmbeddedQueue(File directory, int maxSegmentSize, long addSegmentMaxWaitTimeMs) {
        this.directory = directory;
        this.maxSegmentSize = maxSegmentSize;
        this.addSegmentMaxWaitTimeMs = addSegmentMaxWaitTimeMs;
        this.queue = new MpscLinkedQueue<>();
        this.store = new Store();
    }

    public void requestArrived(Reader reader) {
        queue.offer(reader);
        drain();
    }

    public Reader addReader(long since, OutputStream out) {
        return new Reader(since, out, this);
    }

    public boolean addMessage(long time, byte[] message) {
        if (store.writer.segment == null) {
            AddSegment addSegment = requestCreateSegment();
            drain();
            waitFor(addSegment);
        }
        store.writer.segment.size += message.length + LENGTH_NUM_BYTES + CHECKSUM_NUM_BYTES;
        if (store.writer.segment.size > maxSegmentSize) {
            store.writer.segment.closeForWrite();
            requestCreateSegment();
        }
        store.writer.segment.write(time, message);
        queue.offer(store.writer.segment);
        drain();
        return true;
    }

    private void waitFor(AddSegment addSegment) {
        try {
            boolean added = addSegment.latch.await(addSegmentMaxWaitTimeMs, TimeUnit.MILLISECONDS);
            if (!added) {
                throw new RuntimeException("could not create new segment");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private AddSegment requestCreateSegment() {
        Segment segment = createSegment();
        store.writer.segment = segment;
        AddSegment addSegment = new AddSegment(segment);
        queue.offer(addSegment);
        return addSegment;
    }

    static final class AddSegment {
        final Segment segment;
        final CountDownLatch latch = new CountDownLatch(1);

        AddSegment(Segment segment) {
            this.segment = segment;
        }
    }

    private Segment createSegment() {
        fileNumber++;
        String num = prefixWithZeroes(fileNumber + "", 9);
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
        queue.offer(new AddReader(reader));
        drain();
    }

    void drain() {
        if (wip.getAndIncrement() == 0) {
            int missed = 1;
            while (true) {
                Object o;
                while ((o = queue.poll()) != null) {
                    if (o instanceof AddReader) {
                        Reader r = ((AddReader) o).reader;
                        store.add(r);
                        r.scheduleRead();
                    } else if (o instanceof Reader) {
                        // handle request
                        ((Reader) o).scheduleRead();
                    } else if (o instanceof AddSegment) {
                        AddSegment add = (AddSegment) o;
                        store.segments.add(add.segment);
                        add.latch.countDown();
                    }
                    if (o instanceof Segment) {
                        for (Reader reader : store.readers) {
                            if (o == reader.segment) {
                                reader.scheduleRead();
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

    static final class Message {

        final long time;
        final byte[] content;
        final CountDownLatch latch = new CountDownLatch(1);

        Message(long time, byte[] content) {
            this.time = time;
            this.content = content;
        }

        void markAsPersisted() {
            latch.countDown();
        }
    }

    static final class AddReader {

        private final Reader reader;

        AddReader(Reader reader) {
            this.reader = reader;
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
    }

    static final class Segment {
        private static final String READ_WRITE = "rw";

        private static final int LENGTH_ZERO = 0;
        private static final int EOF = -1;

        final File file;
        final File index;
        int size;

        RandomAccessFile writeFile;

        Segment(File file, File index) {
            this.file = file;
            this.index = index;
        }

        // called only by write thread (singleton)
        void write(long time, byte[] message) {
            try {
                if (writeFile == null) {
                    writeFile = new RandomAccessFile(file, READ_WRITE);
                    writeFile.writeInt(LENGTH_ZERO);
                }
                CRC32 crc = new CRC32();
                crc.update(message);
                long position = writeFile.getFilePointer();
                writeFile.write(message);
                writeFile.writeLong(crc.getValue());
                synchronized (this) {
                    writeFile.writeInt(LENGTH_ZERO);
                }
                long position2 = writeFile.getFilePointer();
                writeFile.seek(position - LENGTH_NUM_BYTES);
                synchronized (this) {
                    writeFile.writeInt(message.length);
                }
                // get position ready for next write (just after length bytes)
                writeFile.seek(position2);
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }

        void closeForWrite() {
            if (writeFile != null) {
                try {
                    writeFile.seek(writeFile.getFilePointer() - LENGTH_NUM_BYTES);
                    writeFile.writeInt(EOF);
                    writeFile.close();
                    writeFile = null;
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
            }
        }

    }

    enum ReaderStatus {
        NO_SEGMENT, //
        ACCEPTING_READS, //
        END_OF_SEGMENT;
    }

    public static final class Reader {

        private final long since;
        private final OutputStream out;
        private final EmbeddedQueue eq;
        private final long batchSize;

        private final AtomicBoolean once = new AtomicBoolean(false);
        private final Worker readWorker = Schedulers.io().createWorker();
        private final AtomicLong requested = new AtomicLong();
        Segment segment;

        // synchronized by wip
        private RandomAccessFile f;

        public Reader(long since, OutputStream out, EmbeddedQueue eq, long batchSize) {
            this.since = since;
            this.out = out;
            this.eq = eq;
            this.batchSize = batchSize;
        }

        void read() {
            if (segment == null) {
                return;
            } else {
                if (f == null) {
                    try {
                        f = new RandomAccessFile(segment.file, "rw");
                    } catch (FileNotFoundException e) {
                        throw new IORuntimeException(e);
                    }
                }
                if (!read(requested, batchSize, out)) {
                    // there may be more messages waiting so try again but it should be scheduled so
                    // that other readers can get a turn to emit
                    eq.requestArrived(this);
                }
            }
        }

        private byte[] message = new byte[0];

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
                long r = requested.get();
                long e = 0;
                long attempts = 0;
                boolean result = false;
                try {
                    while (e != r) {
                        long startPosition = f.getFilePointer();
                        int length = f.readInt();
                        if (length == 0) {
                            return true;
                        } else if (length + f.getFilePointer() > f.length()) {
                            reportError("message in file " + segment.file + " at position "
                                    + startPosition + " is longer than the length of the file");
                            advanceToNextFile();
                            result = false;
                            break;
                        } else {
                            // TODO use same buffer for smaller messages too
                            if (message.length != length) {
                                message = new byte[length];
                            }
                            // blocks
                            f.readFully(message);
                            long expected = f.readLong();
                            CRC32 crc = new CRC32();
                            crc.update(message);
                            if (crc.getValue() != expected) {
                                reportError("crc doesn't match for message in file " + segment.file
                                        + " at position " + startPosition);
                            } else {
                                out.write(Util.toBytes(length));
                                out.write(message);
                                e++;
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
                    throw new IORuntimeException(ex);
                }
            }
        }

        private void reportError(String message) {
            System.err.println(message);
        }

        private void advanceToNextFile() {
//TODO
        }

        void scheduleRead() {
            readWorker.schedule(() -> {
                read();
            });
        }

        public void request(long n) {
            if (n <= 0) {
                return; // NOOP
            }
            addRequest(requested, n);

            // indicate that requests exist
            eq.requestArrived(this);
        }

        public void cancel() {
            // TODO
        }

        public void start() {
            if (once.compareAndSet(false, true)) {
                eq.addReader(this);
            }
        }
    }

    static final class NextSegment {
        final Segment segment;
        final Reader reader;

        NextSegment(Segment segment, Reader reader) {
            this.segment = segment;
            this.reader = reader;
        }

    }

}
