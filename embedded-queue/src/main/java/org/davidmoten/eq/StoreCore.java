package org.davidmoten.eq;

import java.nio.ByteBuffer;

import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessageEnd;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.SegmentFull;

import io.reactivex.Scheduler;

public interface StoreCore {

    long writePositionGlobal();

    long segmentStartPositionGlobal();

    int segmentSize();

    void writeInt(int positionLocal, int length);

    void write(int positionLocal, ByteBuffer bb, int length);

    long messageStartPositionGlobal();

    void send(Event event);

    boolean isFirstPart();

    State state();

    Scheduler scheduler();

    enum State implements Event {
        FIRST_PART, WRITTEN_LENGTH, WRITTEN_PADDING, WRITTEN_CONTENT, WRITTEN_CHECKSUM;
    }

    default void handleMessagePart(MessagePart event) {
        int positionLocal = (int) (writePositionGlobal() - segmentStartPositionGlobal());
        if (positionLocal == segmentSize()) {
            send(new SegmentFull());
        } else {
            State state = state();
            while (true) {
                if (state == State.FIRST_PART) {
                    // due to the use of padding if the segment is not full then there is at least 4
                    // bytes available
                    writeInt(positionLocal, event.bb.remaining());
                    state = State.WRITTEN_LENGTH;
                } else if (state == State.WRITTEN_LENGTH) {
                    int bytesToWrite = Math.min(segmentSize() - positionLocal,
                            event.bb.remaining());
                    scheduler().scheduleDirect(() -> {
                        write(positionLocal, event.bb, bytesToWrite);
                        if (bytesToWrite == event.bb.remaining()) {
                            send(State.WRITTEN_CONTENT);
                        } else {
                            // alter the bb
                            event.bb.position(event.bb.position() + bytesToWrite);
                            send(new SegmentFull());
                            send(event);
                        }
                    });
                } else if (state == State.WRITTEN_PADDING) {

                }
            }
        }
    }

    default void handleMessageEnd(MessageEnd event) {

    }

}
