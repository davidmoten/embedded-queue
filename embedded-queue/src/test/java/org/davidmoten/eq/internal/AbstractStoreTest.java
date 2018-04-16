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

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public class AbstractStoreTest {

    @Test
    public void testHandleMessagePart() {
        MyStore store = new MyStore();
        store.state = State.FIRST_PART;
        byte[] msg = "hi".getBytes();
        store.handleMessagePart(new MessagePart(ByteBuffer.wrap(msg)));
        store.records.stream().forEach(System.out::println);
        Checksum c = new CRC32();
        c.update(msg, 0, msg.length);
        Segment segment = store.segment;
        List<Record> r = store.records;
        assertEquals(create(segment, 0, 0), r.get(0)); // write zero length
        assertEquals(create(segment, 4, (byte) 1), r.get(1)); // write padding length
        assertEquals(create(segment, 5, (byte) 0), r.get(2)); // write padding
        assertEquals(create(segment, 6, msg), r.get(3)); // write msg
        assertEquals(create(segment, 8, (int) c.getValue()), r.get(4)); // write checksum
        assertEquals(create(segment, 12, 0), r.get(5)); // write length of next record as zero for
                                                        // readers
        assertEquals(create(segment, 0, 2), r.get(6)); // rewrite length of message, now ready for
                                                       // readers
        assertEquals(7, r.size());
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
            final int prime = 31;
            int result = 1;
            result = prime * result + ((object == null) ? 0 : object.hashCode());
            result = prime * result + positionLocal;
            result = prime * result + ((segment == null) ? 0 : segment.hashCode());
            return result;
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

        Segment segment = new Segment(new File("target/s1"), 0);
        List<Record> records = new ArrayList<>();

        @Override
        int segmentSize() {
            return 100;
        }

        @Override
        Segment writeSegment() {
            return segment;
        }

        @Override
        void writeInt(int positionLocal, int value) {
            records.add(new Record(segment, positionLocal, value));
        }

        @Override
        void writeByte(int positionLocal, int value) {
            records.add(new Record(segment, positionLocal, (byte) value));
        }

        @Override
        void write(int positionLocal, ByteBuffer bb, int length) {
            byte[] bytes = new byte[length];
            int p = bb.position();
            bb.get(bytes);
            bb.position(p);
            records.add(new Record(segment, positionLocal, bytes));
        }

        @Override
        void writeInt(Segment segment, int positionLocal, int value) {
            records.add(new Record(segment, positionLocal, value));
        }

        @Override
        void send(Event event) {
            if (event instanceof SegmentFull) {
                segment = new Segment(new File("target/s2"), segment.start + segmentSize());
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
