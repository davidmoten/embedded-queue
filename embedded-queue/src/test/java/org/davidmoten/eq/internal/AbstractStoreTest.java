package org.davidmoten.eq.internal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.davidmoten.eq.internal.AbstractStore.State;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessagePart;
import org.junit.Test;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public class AbstractStoreTest {

    @Test
    public void testCreateSegment() {
        MyStore store = new MyStore();
        store.state = State.FIRST_PART;
        store.handleMessagePart(new MessagePart(ByteBuffer.wrap("hi".getBytes())));
        store.records.stream().forEach(System.out::println);
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
            return "Record [segment=" + segment + ", positionLocal=" + positionLocal + ", object="
                    + toString(object) + "]";
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

        Segment segment = new Segment(new File("target/t"), 0);
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

        }

        @Override
        void send(Event event1, Event event2) {

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
