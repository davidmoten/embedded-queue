package org.davidmoten.eq.internal;

import java.io.File;
import java.nio.ByteBuffer;

import org.davidmoten.eq.internal.AbstractStore.State;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessagePart;
import org.junit.Test;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public class AbstractStoreTest {

    @Test
    public void testCreateSegment() {
        AbstractStore store = create();
        store.state = State.FIRST_PART;
        store.handleMessagePart(new MessagePart(ByteBuffer.wrap("hi".getBytes())));
    }

    private static AbstractStore create() {
        return new AbstractStore() {

            Segment segment = new Segment(new File("target/t"), 0);

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

            }

            @Override
            void writeByte(int positionLocal, int value) {

            }

            @Override
            void write(int positionLocal, ByteBuffer bb, int length) {

            }

            @Override
            void writeInt(Segment segment, int positionLocal, int value) {

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
        };
    }

}
