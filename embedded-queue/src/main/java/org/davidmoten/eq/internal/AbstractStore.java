package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.davidmoten.eq.internal.Segment;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.SegmentCreated;
import org.davidmoten.eq.internal.event.SegmentFull;

import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

import io.reactivex.Scheduler;

public abstract class AbstractStore {
    
    private final int segmentSize;
    private final Scheduler scheduler;

    public AbstractStore(int segmentSize, Scheduler scheduler) {
        this.segmentSize = segmentSize;
        this.scheduler = scheduler;
        
    }

    abstract Segment writeSegment();

    abstract void writeInt(int positionLocal, int value);

    abstract void writeByte(int positionLocal, int value);

    abstract void write(int positionLocal, ByteBuffer bb, int length);

    abstract void writeInt(Segment segment, int positionLocal, int value);

    abstract void send(Event event);

    // avoid two drains by offering this method
    abstract void send(Event event1, Event event2);

    abstract Segment createSegment(long positionGlobal);

    public enum State {
        FIRST_PART, WRITTEN_LENGTH, WRITTEN_PADDING, WRITTEN_CONTENT, FULL_SEGMENT;
    }

    private final Checksum checksum = new CRC32();

    int contentLength;
    long writePositionGlobal;
    Segment messageStartSegment;
    int messageStartPositionLocal;

    State state = State.FULL_SEGMENT;

    private static final int CHECKSUM_BYTES = 4;

    private static final int LENGTH_BYTES = 4;

    public final void handleSegmentFull(SegmentFull event) {
        scheduler.scheduleDirect(new SegmentFullHandler(event));
    }

    public final void handleMessagePart(MessagePart event) {
        final int entryPositionLocal = (int) (writePositionGlobal - messageStartPositionLocal);
        if (entryPositionLocal == segmentSize) {
            send(new SegmentFull(event));
        } else {
            State entryState = this.state;
            scheduler.scheduleDirect(new MessagePartHandler(event, entryPositionLocal, entryState));
        }
    }

    private final class SegmentFullHandler implements Runnable {

        private final SegmentFull event;

        SegmentFullHandler(SegmentFull event) {
            this.event = event;
        }

        @Override
        public void run() {
            Segment segment = createSegment(writePositionGlobal);
            SegmentCreated event1 = new SegmentCreated(segment, segmentSize);
            if (event.messagePart == null) {
                send(event1);
            } else {
                send(event1, event.messagePart);
            }
        }

    }

    private final class MessagePartHandler implements Runnable {

        private final MessagePart event;
        private final int entryPositionLocal;
        private final State entryState;

        MessagePartHandler(MessagePart event, int entryPositionLocal, State entryState) {
            this.event = event;
            this.entryPositionLocal = entryPositionLocal;
            this.entryState = entryState;
        }

        @Override
        public void run() {
            try {
                State state = entryState;
                int positionLocal = entryPositionLocal;
                Event sendEvent = null;
                while (true) {
                    if (positionLocal == segmentSize) {
                        sendEvent = new SegmentFull(event);
                        break;
                    }
                    if (state == State.FIRST_PART) {
                        /////////////////////////////
                        // write length
                        /////////////////////////////
                        // due to the use of padding if the segment is not full then there is at
                        // least 4 bytes available
                        // write 0 in the length field till writing finished and we will come
                        // back and overwrite it with content length
                        writeInt(positionLocal, 0);
                        messageStartSegment = writeSegment();
                        messageStartPositionLocal = positionLocal;
                        positionLocal += LENGTH_BYTES;
                        checksum.reset();
                        contentLength = 0;
                        state = State.WRITTEN_LENGTH;
                    } else if (state == State.WRITTEN_LENGTH) {
                        /////////////////////////////
                        // write padding
                        /////////////////////////////
                        int paddingBytes = 3 - event.bb.remaining() % 4;
                        writeByte(positionLocal, paddingBytes);
                        for (int i = 1; i <= paddingBytes; i++) {
                            writeByte(positionLocal + i, 0);
                        }
                        positionLocal += paddingBytes + 1;
                        state = State.WRITTEN_PADDING;
                    } else if (state == State.WRITTEN_PADDING) {
                        /////////////////////////////
                        // write content (or continue writing content)
                        /////////////////////////////
                        int bytesToWrite = Math.min(segmentSize - positionLocal, event.bb.remaining());
                        write(positionLocal, event.bb, bytesToWrite);
                        updateChecksum(checksum, event.bb, bytesToWrite);
                        positionLocal += bytesToWrite;
                        contentLength += bytesToWrite;
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
                        // ensure the length field of the next item is set to zero
                        // if is end of segment then don't need to do it
                        if (positionLocal < segmentSize) {
                            writeInt(positionLocal, 0);
                        }
                        // rewrite the length field at the start of the message
                        writeInt(messageStartSegment, messageStartPositionLocal, contentLength);
                        state = State.FIRST_PART;
                        break;
                    }
                }
                writePositionGlobal = positionLocal - entryPositionLocal;
                AbstractStore.this.state = state;
                if (sendEvent != null) {
                    send(new SegmentFull(event));
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    // TODO test
    @VisibleForTesting
    static void updateChecksum(Checksum checksum, ByteBuffer bb, int bytesToWrite) {
        if (bb.hasArray()) {
            checksum.update(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
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
