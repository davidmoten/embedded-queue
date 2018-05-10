package org.davidmoten.eq.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.davidmoten.eq.internal.WriteHandler.State;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessageEnd;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.Part;
import org.davidmoten.eq.internal.event.SegmentCreated;
import org.davidmoten.eq.internal.event.SegmentFull;
import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.schedulers.Schedulers;

public class WriteHandlerTest {

    @Test
    public void testHandleMessagePartUsePartSegment() {
        final MyStore store = new MyStore(100);
        store.writeHandler.state = State.FIRST_PART;
        final byte[] msg = "hi".getBytes();
        final int start = 4;
        store.writeHandler.writePositionGlobal = start;
        store.writeHandler.handlePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.writeHandler.handlePart(MessageEnd.instance());
        store.records.stream().forEach(System.out::println);
        final Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        final Segment segment = store.writeSegment();
        final List<Record> r = store.records;
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
        assertTrue(store.closed.isEmpty());
        assertNull(store.error);
    }

    @Test
    public void testHandleMessagePartUseWholeSegment() {
        final MyStore store = new MyStore(12);
        store.writeHandler.state = State.FIRST_PART;
        final byte[] msg = "hi".getBytes();
        store.writeHandler.handlePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.writeHandler.handlePart(MessageEnd.instance());
        store.records.stream().forEach(System.out::println);
        final Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        final Segment segment = store.writeSegment();
        final List<Record> r = store.records;
        assertEquals(create(segment, 0, 0), r.get(0)); // write zero length
        assertEquals(create(segment, 4, (byte) 1), r.get(1)); // write padding length
        assertEquals(create(segment, 5, (byte) 0), r.get(2)); // write padding
        assertEquals(create(segment, 6, msg), r.get(3)); // write msg
        assertEquals(create(segment, 8, (int) c.getValue()), r.get(4)); // write checksum
        // don't write zero length for next record as is end of segment and we assume
        // that each new segment
        // has a zeroed first 4 bytes
        // readers
        assertEquals(create(segment, 0, 2), r.get(5)); // rewrite length of message, now ready for
                                                       // readers
        assertEquals(6, r.size());
        assertTrue(store.closed.isEmpty());
        assertNull(store.error);
    }

    @Test
    public void testHandleMessagePartChecksumInNextSegment() {
        final MyStore store = new MyStore(8);
        store.writeHandler.state = State.FIRST_PART;
        final byte[] msg = "hi".getBytes();
        store.writeHandler.handlePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.writeHandler.handlePart(MessageEnd.instance());
        store.records.stream().forEach(System.out::println);
        final Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        final Segment segment1 = store.segments.get(0);
        final Segment segment2 = store.segments.get(1);
        final List<Record> r = store.records;
        System.out.println(store.segments);
        assertEquals(create(segment1, 0, 0), r.get(0)); // write zero length
        assertEquals(create(segment1, 4, (byte) 1), r.get(1)); // write padding length
        assertEquals(create(segment1, 5, (byte) 0), r.get(2)); // write padding
        assertEquals(create(segment1, 6, msg), r.get(3)); // write msg (2 bytes)
        assertEquals(create(segment2, 0, (int) c.getValue()), r.get(4)); // write checksum
        assertEquals(create(segment2, 4, 0), r.get(5)); // write length 0 for next record
        assertEquals(create(segment1, 0, 2), r.get(6)); // rewrite length of message, now ready for
                                                        // readers
        assertEquals(7, r.size());
        assertEquals(Collections.singletonList(segment1), store.closed);
        assertNull(store.error);
    }

    @Test
    public void testHandleMessagePartContentSplitAcrossTwoSegments() {
        final MyStore store = new MyStore(8);
        store.writeHandler.state = State.FIRST_PART;

        // 6 bytes, 2 of which will be written to first segment
        // and the other 4 will be at the start of the second segment
        final byte[] msg = "hi1234".getBytes();
        store.writeHandler.handlePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.writeHandler.handlePart(MessageEnd.instance());
        store.records.stream().forEach(System.out::println);
        final Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        final Segment segment1 = store.segments.get(0);
        final Segment segment2 = store.segments.get(1);
        final List<Record> r = store.records;
        System.out.println(store.segments);
        assertEquals(create(segment1, 0, 0), r.get(0)); // write zero length
        assertEquals(create(segment1, 4, (byte) 1), r.get(1)); // write padding length
        assertEquals(create(segment1, 5, (byte) 0), r.get(2)); // write padding
        assertEquals(create(segment1, 6, Arrays.copyOfRange(msg, 0, 2)), r.get(3)); // write msg (2 bytes)
        assertEquals(create(segment2, 0, Arrays.copyOfRange(msg, 2, 6)), r.get(4)); // write msg (2 bytes)
        assertEquals(create(segment2, 4, (int) c.getValue()), r.get(5)); // write checksum
        assertEquals(create(segment1, 0, 6), r.get(6)); // rewrite length of message, now ready for
                                                        // readers
        assertEquals(7, r.size());
        assertEquals(Collections.singletonList(segment1), store.closed);
    }

    @Test
    public void testWriteDelimiterAfterEveryRecord() {
        final MyStore store = new MyStore(200, 1);
        store.writeHandler.state = State.FIRST_PART;
        final byte[] msg = "hi".getBytes();
        final int start = 4;
        store.writeHandler.writePositionGlobal = start;
        store.writeHandler.handlePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.writeHandler.handlePart(MessageEnd.instance());
        store.records.stream().forEach(System.out::println);
        final Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        final Segment segment = store.writeSegment();
        final List<Record> r = store.records;
        assertEquals(create(segment, start + 0, 0), r.get(0)); // write zero length
        assertEquals(create(segment, start + 4, (byte) 1), r.get(1)); // write padding length
        assertEquals(create(segment, start + 5, (byte) 0), r.get(2)); // write padding
        assertEquals(create(segment, start + 6, msg), r.get(3)); // write msg
        assertEquals(create(segment, start + 8, (int) c.getValue()), r.get(4)); // write checksum
        assertEquals(create(segment, start + 12, 0), r.get(5)); // write length of next record as
        assertEquals(create(segment, start + 0, 2), r.get(6)); // rewrite length of message, now ready for readers
        assertEquals(create(segment, start + 12, 0), r.get(7)); // write length of next record as
        // zero for delimiter

        assertEquals(create(segment, start + 16, Delimiter.REMAINING), r.get(8)); // rewrite length of message, now
        assertEquals(create(segment, start + 12, Delimiter.START), r.get(9)); // rewrite length of message, now
        // ready for readers
        assertEquals(10, r.size());
        assertTrue(store.closed.isEmpty());
        assertNull(store.error);
    }

    @Test
    public void testHandleMessagePartContentSplitAcrossFourSegments() {
        final MyStore store = new MyStore(8);
        store.writeHandler.state = State.FIRST_PART;

        // 16 bytes, 2 of which will be written to first segment
        // and the other 4 will be at the start of the second segment
        final byte[] msg = "1234567890123456".getBytes();
        store.writeHandler.handlePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.writeHandler.handlePart(MessageEnd.instance());
        store.records.stream().forEach(System.out::println);
        final Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        final Segment segment1 = store.segments.get(0);
        final Segment segment2 = store.segments.get(1);
        final Segment segment3 = store.segments.get(2);
        final Segment segment4 = store.segments.get(3);
        final Iterator<Record> r = store.records.iterator();
        System.out.println(store.segments);
        assertEquals(create(segment1, 0, 0), r.next()); // write zero length
        assertEquals(create(segment1, 4, (byte) 3), r.next()); // write padding length
        assertEquals(create(segment1, 5, (byte) 0), r.next()); // write padding
        assertEquals(create(segment1, 6, (byte) 0), r.next()); // write padding
        assertEquals(create(segment1, 7, (byte) 0), r.next()); // write padding
        assertEquals(create(segment2, 0, Arrays.copyOfRange(msg, 0, 8)), r.next()); // write msg (2 bytes)
        assertEquals(create(segment3, 0, Arrays.copyOfRange(msg, 8, 16)), r.next()); // write msg (2 bytes)
        assertEquals(create(segment4, 0, (int) c.getValue()), r.next()); // write checksum
        assertEquals(create(segment4, 4, 0), r.next()); // write zero length to next record
        assertEquals(create(segment1, 0, 16), r.next()); // rewrite length of message, now ready for
                                                         // readers
        assertFalse(r.hasNext());
        assertEquals(Arrays.asList(segment2, segment3, segment1), store.closed);
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
            final Record other = (Record) obj;
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

    private static final class MyStore implements StoreWriter {

        final List<Segment> segments = Lists.newArrayList(new Segment(new File("target/s1"), 0));
        final List<Record> records = new ArrayList<>();
        final List<Segment> closed = new ArrayList<>();
        final WriteHandler writeHandler;
        Throwable error;
        private final int segmentSize;

        MyStore(int segmentSize) {
            this(segmentSize, 0);
        }

        MyStore(int segmentSize, int delimitEvery) {
            this.segmentSize = segmentSize;
            Preconditions.checkArgument(segmentSize % 4 == 0);
            this.writeHandler = new WriteHandler(this, segmentSize, delimitEvery, Schedulers.trampoline());
        }

        @Override
        public Segment writeSegment() {
            return segments.get(segments.size() - 1);
        }

        @Override
        public void writeInt(int positionLocal, int value) {
            checkPositionLocal(positionLocal);
            records.add(new Record(writeSegment(), positionLocal, value));
        }

        private void checkPositionLocal(int positionLocal) {
            Preconditions.checkArgument(positionLocal < segmentSize);
        }

        @Override
        public void writeByte(int positionLocal, int value) {
            checkPositionLocal(positionLocal);
            records.add(new Record(writeSegment(), positionLocal, (byte) value));
        }

        @Override
        public void write(int positionLocal, ByteBuffer bb, int length) {
            checkPositionLocal(positionLocal);
            final byte[] bytes = new byte[length];
            final int p = bb.position();
            bb.get(bytes);
            bb.position(p);
            records.add(new Record(writeSegment(), positionLocal, bytes));
        }

        @Override
        public void writeInt(Segment segment, int positionLocal, int value) {
            checkPositionLocal(positionLocal);
            records.add(new Record(segment, positionLocal, value));
        }

        @Override
        public void send(Event event) {
            if (event instanceof SegmentFull) {
                writeHandler.handleSegmentFull((SegmentFull) event);
            } else if (event instanceof Part) {
                writeHandler.handlePart((Part) event);
            } else if (event instanceof SegmentCreated) {
                writeHandler.handleSegmentCreated((SegmentCreated) event);
            }
        }

        @Override
        public void send(Event event1, Event event2) {
            send(event1);
            send(event2);
        }

        @Override
        public Segment createSegment(long positionGlobal) {
            return new Segment(new File("target/" + positionGlobal), positionGlobal);
        }

        @Override
        public void addSegment(Segment segment) {
            segments.add(segment);
        }

        @Override
        public void closeForWrite(Segment segment) {
            closed.add(segment);
        }

        @Override
        public void errorOccurred(Throwable error) {
            this.error = error;
        }
    }
}
