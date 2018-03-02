package org.davidmoten.eq;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.reactivex.Scheduler.Worker;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.schedulers.Schedulers;

public final class EmbeddedQueue {

    private final SimplePlainQueue<Object> queue;
    private final Store store;
    private final AtomicInteger wip = new AtomicInteger();

    public EmbeddedQueue() {
        this.queue = new MpscLinkedQueue<>();
        this.store = new Store();
    }

    public void requestArrived(Reader reader) {
        queue.offer(reader);
        drain();
    }

    public Reader add(long since, OutputStream out) {
        return new Reader(since, out, this);
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
                    } else if (o instanceof Reader) {
                        // handle request
                        ((Reader) o).scheduleRead();
                    }
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    static final class AddReader {

        private final Reader reader;

        AddReader(Reader reader) {
            this.reader = reader;
        }

    }

    static final class Store {

        final Writer writer;
        final List<Reader> readers;
        final List<Segment> segments;

        Store() {
            this.writer = new Writer();
            this.readers = new ArrayList<>();
            this.segments = new ArrayList<>();
        }

        public void add(Reader reader) {
            readers.add(reader);
        }
        
        public void addSegment() {

        }
    }

    public static final class Writer {

    }

    static final class Segment {
        final File file;
        final File index;

        Segment(File file, File index) {
            this.file = file;
            this.index = index;
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
        private final AtomicBoolean once = new AtomicBoolean(false);
        private final Worker readWorker = Schedulers.io().createWorker();
        private final AtomicLong requested = new AtomicLong();

        // mutable, synchronized by EmbeddedQueue.wip
        ReaderStatus status;

        public Reader(long since, OutputStream out, EmbeddedQueue eq) {
            this.since = since;
            this.out = out;
            this.eq = eq;
        }

        void read() {

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
            // CAS loop
            while (true) {
                long r = requested.get();
                if (r == Long.MAX_VALUE) {
                    break;
                } else {
                    long r2 = r + n;
                    if (r2 < 0) {
                        r2 = Long.MAX_VALUE;
                    }
                    if (requested.compareAndSet(r, r2)) {
                        break;
                    }
                }
            }
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

}
