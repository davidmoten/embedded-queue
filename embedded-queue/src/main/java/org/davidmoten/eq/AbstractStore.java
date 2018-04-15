package org.davidmoten.eq;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.davidmoten.eq.internal.Segment;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.SegmentFull;

import io.reactivex.Scheduler;

public abstract class AbstractStore {

    abstract int segmentSize();

    abstract void writeInt(int positionLocal, int value);

    abstract void writeByte(int positionLocal, int value);

    abstract void write(int positionLocal, ByteBuffer bb, int length);

    abstract void writeInt(Segment segment, int positionLocal, int value);

    abstract void send(Event event);

    abstract void drain();

    abstract boolean isFirstPart();

    abstract State state();

    abstract Scheduler scheduler();

    public enum State implements Event {
        FIRST_PART, WRITTEN_LENGTH, WRITTEN_PADDING, WRITTEN_CONTENT, WRITTEN_CHECKSUM;
    }

    abstract Info info();

    public final static class Info {

        public final long writePositionGlobal;
        public final Segment messageStartSegment;
        public int messageStartPositionLocal;
        public final boolean isFirstPart;
        public final State state;

        public Info(long writePositionGlobal, Segment messageStartSegment, int messageStartPositionLocal,
                 boolean isFirstPart, State state) {
            this.writePositionGlobal = writePositionGlobal;
            this.messageStartSegment = messageStartSegment;
            this.messageStartPositionLocal = messageStartPositionLocal;
            this.isFirstPart = isFirstPart;
            this.state = state;
        }
    }

    private final Checksum checksum = new CRC32();
    private int contentLength;
    private static final int CHECKSUM_BYTES = 4;

    public final void handleMessagePart(MessagePart event) {
        Info info = info();
        final int entryPositionLocal = (int) (info.writePositionGlobal
                - info.messageStartPositionLocal);
        if (entryPositionLocal == segmentSize()) {
            send(new SegmentFull(event, info));
        } else {
            State entryState = state();
            scheduler().scheduleDirect(() -> {
                State state = entryState;
                int positionLocal = entryPositionLocal;
                while (true) {
                    if (positionLocal == segmentSize()) {
                        send(new SegmentFull(event, info));
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
                        contentLength = 0;
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
                        // write content (or continue writing content)
                        /////////////////////////////
                        int bytesToWrite = Math.min(segmentSize() - positionLocal,
                                event.bb.remaining());
                        write(positionLocal, event.bb, bytesToWrite);
                        updateChecksum(checksum, event.bb, bytesToWrite);
                        if (bytesToWrite == event.bb.remaining()) {
                            state = State.WRITTEN_CONTENT;
                        } else {
                            // alter the bb
                            event.bb.position(event.bb.position() + bytesToWrite);
                        }
                    } else if (state == State.WRITTEN_CONTENT) {
                        /////////////////////////////
                        // write checksum
                        /////////////////////////////
                        writeInt(positionLocal, (int) checksum.getValue());
                        positionLocal += CHECKSUM_BYTES;
                        state = State.WRITTEN_CHECKSUM;
                        writeInt(info.messageStartSegment, info.messageStartPositionLocal, contentLength);
                    }
                }
            });
        }
    }

    private static void updateChecksum(Checksum checksum, ByteBuffer bb, int bytesToWrite) {
        if (bb.hasArray()) {
            checksum.update(bb.array(),
                    bb.arrayOffset() + bb.position(),
                    bb.remaining());
        } else {
            int p = bb.position();
            for (int i = 0; i < bytesToWrite; i++) {
                checksum.update(bb.get());
            }
            // revert the position changed by calling bb.get()
            bb.position(p);
        }
    }

}
