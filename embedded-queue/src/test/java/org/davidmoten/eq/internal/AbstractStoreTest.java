package org.davidmoten.eq.internal;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.davidmoten.eq.internal.AbstractStore.State;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.SegmentFull;
import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public class AbstractStoreTest {

    @Test
    public void testHandleMessagePartUsePartSegment() {
        MyStore store = new MyStore(100);
        store.state = State.FIRST_PART;
        byte[] msg = "hi".getBytes();
        int start = 4;
        store.writePositionGlobal = start;
        store.handleMessagePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.records.stream().forEach(System.out::println);
        Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        Segment segment = store.writeSegment();
        List<Record> r = store.records;
        assertEquals(create(segment, start + 0, 0), r.get(0)); // write zero length
        assertEquals(create(segment, start + 4, (byte) 1), r.get(1)); // write padding length
        assertEquals(create(segment, start + 5, (byte) 0), r.get(2)); // write padding
        assertEquals(create(segment, start + 6, msg), r.get(3)); // write msg
        assertEquals(create(segment, start + 8, (int) c.getValue()), r.get(4)); // write checksum
        assertEquals(create(segment, start + 12, 0), r.get(5)); // write length of next record as
                                                                // zero for
        // readers
        assertEquals(create(segment, start + 0, 2), r.get(6)); // rewrite length of message, now ready for
        // readers
        assertEquals(7, r.size());
    }

    @Test
    public void testHandleMessagePartUseWholeSegment() {
        MyStore store = new MyStore(12);
        store.state = State.FIRST_PART;
        byte[] msg = "hi".getBytes();
        store.handleMessagePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.records.stream().forEach(System.out::println);
        Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        Segment segment = store.writeSegment();
        List<Record> r = store.records;
        assertEquals(create(segment, 0, 0), r.get(0)); // write zero length
        assertEquals(create(segment, 4, (byte) 1), r.get(1)); // write padding length
        assertEquals(create(segment, 5, (byte) 0), r.get(2)); // write padding
        assertEquals(create(segment, 6, msg), r.get(3)); // write msg
        assertEquals(create(segment, 8, (int) c.getValue()), r.get(4)); // write checksum
        //don't write zero length for next record as is end of segment and we assume that each new segment 
        // has a zeroed first 4 bytes
        // readers
        assertEquals(create(segment, 0, 2), r.get(5)); // rewrite length of message, now ready for
                                                       // readers
        assertEquals(6, r.size());
    }

    private static Record create(Segment segment, int positionLocal, Object o) {
        return new Record(segment, positionLocal, o);
    }

    private static final class Record {

        final Segment segment;
        final int positionLocal;
        final Object object;

        Record(Segment segment, int positionLocal, Object object) {
            this.segment = segment;
            this.positionLocal = positionLocal;
            this.object = object;
        }

        @Override
        public String toString() {
            return "Record [segment=" + segment + ", positionLocal=" + positionLocal + ", object<"
                    + object.getClass().getSimpleName() + ">=" + toString(object) + "]";
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Record other = (Record) obj;
            if (object == null) {
                if (other.object != null)
                    return false;
            } else if (!equals(object, other.object))
                return false;
            if (positionLocal != other.positionLocal)
                return false;
            if (segment == null) {
                if (other.segment != null)
                    return false;
            } else if (!segment.equals(other.segment))
                return false;
            return true;
        }

        private boolean equals(Object a, Object b) {
            if (a == null || b == null) {
                return a == b;
            } else {
                if (a.getClass().isArray()) {
                    if (b.getClass().isArray()) {
                        return Arrays.equals((byte[]) a, (byte[]) b);
                    } else {
                        return false;
                    }
                } else {
                    return a.equals(b);
                }
            }
        }

        private static String toString(Object o) {
            if (o.getClass().isArray()) {
                return Arrays.toString((byte[]) o);
            } else {
                return String.valueOf(o);
            }
        }
    }

    private static final class MyStore extends AbstractStore {

        List<Segment> segments = Lists.newArrayList(new Segment(new File("target/s1"), 0));
        List<Record> records = new ArrayList<>();
        private final int segmentSize;

        MyStore(int segmentSize) {
            this.segmentSize = segmentSize;
        }

        @Override
        int segmentSize() {
            return segmentSize;
        }

        @Override
        Segment writeSegment() {
            return segments.get(segments.size() - 1);
        }

        @Override
        void writeInt(int positionLocal, int value) {
            checkPositionLocal(positionLocal);
            records.add(new Record(writeSegment(), positionLocal, value));
        }

        private void checkPositionLocal(int positionLocal) {
            Preconditions.checkArgument(positionLocal < segmentSize());
        }

        @Override
        void writeByte(int positionLocal, int value) {
            checkPositionLocal(positionLocal);
            records.add(new Record(writeSegment(), positionLocal, (byte) value));
        }

        @Override
        void write(int positionLocal, ByteBuffer bb, int length) {
            checkPositionLocal(positionLocal);
            byte[] bytes = new byte[length];
            int p = bb.position();
            bb.get(bytes);
            bb.position(p);
            records.add(new Record(writeSegment(), positionLocal, bytes));
        }

        @Override
        void writeInt(Segment segment, int positionLocal, int value) {
            checkPositionLocal(positionLocal);
            records.add(new Record(segment, positionLocal, value));
        }

        @Override
        void send(Event event) {
            if (event instanceof SegmentFull) {
                segments.add(new Segment(new File("target/s2"), writeSegment().start + segmentSize()));
            } else if (event instanceof MessagePart) {
                handleMessagePart((MessagePart) event);
            }
        }

        @Override
        void send(Event event1, Event event2) {
            send(event1);
            send(event2);
        }

        @Override
        Scheduler scheduler() {
            return Schedulers.trampoline();
        }

        @Override
        Segment createSegment(long positionGlobal) {
            return new Segment(new File("target/" + positionGlobal), positionGlobal);
        }
    }

}
