package org.davidmoten.eq;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessageEnd;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.SegmentFull;

import io.reactivex.Scheduler;

public abstract class StoreCore {

    abstract long writePositionGlobal();

    abstract long segmentStartPositionGlobal();

    abstract int segmentSize();

    abstract void writeInt(int positionLocal, int value);

    abstract void writeByte(int positionLocal, int value);

    abstract void write(int positionLocal, ByteBuffer bb, int length);

    abstract long messageStartPositionGlobal();

    abstract void send(Event event);

    abstract void send(Event event1, Event event2);

    abstract void drain();

    abstract boolean isFirstPart();

    abstract State state();

    abstract Scheduler scheduler();

    enum State implements Event {
        FIRST_PART, WRITTEN_LENGTH, WRITTEN_PADDING, WRITTEN_CONTENT, WRITTEN_CHECKSUM;
    }

    private final Checksum checksum = new CRC32();
    private static final int CHECKSUM_BYTES = 4;

    public final void handleMessagePart(MessagePart event) {
        final int entryPositionLocal = (int) (writePositionGlobal() - segmentStartPositionGlobal());
        if (entryPositionLocal == segmentSize()) {
            send(new SegmentFull(event));
        } else {
            State entryState = state();
            scheduler().scheduleDirect(() -> {
                State state = entryState;
                int positionLocal = entryPositionLocal;
                while (true) {
                    if (positionLocal == segmentSize()) {
                        send(new SegmentFull(event));
                        break;
                    }
                    if (state == State.FIRST_PART) {
                        /////////////////////////////
                        // write length
                        /////////////////////////////
                        // due to the use of padding if the segment is not full then there is at
                        // least 4 bytes available
                        writeInt(positionLocal, event.bb.remaining());
                        positionLocal += event.bb.remaining();
                        checksum.reset();
                        state = State.WRITTEN_LENGTH;
                    } else if (state == State.WRITTEN_LENGTH) {
                        /////////////////////////////
                        // write padding
                        /////////////////////////////
                        // pad
                        int paddingBytes = 3 - event.bb.remaining() % 4;
                        writeByte(positionLocal, paddingBytes);
                        for (int i = 1; i <= paddingBytes; i++) {
                            writeByte(positionLocal + i, 0);
                        }
                        positionLocal += paddingBytes;
                        state = State.WRITTEN_PADDING;
                    } else if (state == State.WRITTEN_PADDING) {
                        /////////////////////////////
                        // write content
                        /////////////////////////////
                        int bytesToWrite = Math.min(segmentSize() - positionLocal,
                                event.bb.remaining());
                        write(positionLocal, event.bb, bytesToWrite);
                        if (event.bb.hasArray()) {
                            checksum.update(event.bb.array(),
                                    event.bb.arrayOffset() + event.bb.position(),
                                    event.bb.remaining());
                        }
                        if (bytesToWrite == event.bb.remaining()) {
                            state = State.WRITTEN_CONTENT;
                        } else {
                            // alter the bb
                            event.bb.position(event.bb.position() + bytesToWrite);
                            // TODO send new position
                            send(new SegmentFull(event));
                            break;
                        }
                    } else if (state == State.WRITTEN_CONTENT) {
                        /////////////////////////////
                        // write checksum
                        /////////////////////////////
                        writeInt(positionLocal, (int) checksum.getValue());
                        positionLocal += CHECKSUM_BYTES;
                        state = State.WRITTEN_CHECKSUM;
                        break;
                    }
                }
            });
        }
    }

}
