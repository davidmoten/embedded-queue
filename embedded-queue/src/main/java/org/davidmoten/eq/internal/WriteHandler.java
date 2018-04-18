package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.davidmoten.eq.internal.event.EndWritten;
import org.davidmoten.eq.internal.event.Event;
import org.davidmoten.eq.internal.event.MessageEnd;
import org.davidmoten.eq.internal.event.MessagePart;
import org.davidmoten.eq.internal.event.Part;
import org.davidmoten.eq.internal.event.SegmentCreated;
import org.davidmoten.eq.internal.event.SegmentFull;

import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

import io.reactivex.Scheduler;

public final class WriteHandler {

    private final int segmentSize;
    private final Scheduler scheduler;
    private final StoreWriter storeWriter;

    public WriteHandler(StoreWriter storeWriter, int segmentSize, Scheduler scheduler) {
        this.storeWriter = storeWriter;
        this.segmentSize = segmentSize;
        this.scheduler = scheduler;

    }

    public enum State {
        FIRST_PART, WRITTEN_LENGTH, WRITING_CONTENT, WRITTEN_CONTENT;
    }

    private final Checksum checksum = new CRC32();

    int contentLength;

    /**
     * The value where the next part will start writing. This is the long value used
     * as a position across all segments. Each segment is named with its start write
     * position.
     */
    long writePositionGlobal;

    /**
     * The start of the message currently being written may be in a previous
     * segment. Once the last bytes of the message are written (and its crc32) then
     * the length field (first 4 bytes of the message) are rewritten with the actual
     * value to indicate to a reader that it can consume that message.
     */
    Segment messageStartSegment;

    int messageStartPositionLocal;

    State state = State.FIRST_PART;

    private static final int CHECKSUM_BYTES = 4;

    private static final int LENGTH_BYTES = 4;

    public void handleSegmentFull(SegmentFull event) {
        // blocking operations must be scheduled on io scheduler
        scheduler.scheduleDirect(new SegmentFullHandler(event));
    }

    public void handleSegmentCreated(SegmentCreated event) {
        // don't have to schedule this as is non-blocking
        storeWriter.addSegment(event.segment);
    }

    public void handlePart(Part event) {
        final int entryPositionLocal = (int) (writePositionGlobal - storeWriter.writeSegment().start);
        if (entryPositionLocal == segmentSize) {
            storeWriter.send(new SegmentFull(event));
        } else {
            State entryState = this.state;
            // blocking operations must be scheduled on io scheduler
            scheduler.scheduleDirect(new PartHandler(event, entryPositionLocal, entryState));
        }
    }

    private final class SegmentFullHandler implements Runnable {

        private final SegmentFull event;

        SegmentFullHandler(SegmentFull event) {
            this.event = event;
        }

        @Override
        public void run() {
            try {
                if (messageStartSegment != storeWriter.writeSegment()) {
                    storeWriter.closeForWrite(storeWriter.writeSegment());
                }
                Segment segment = storeWriter.createSegment(writePositionGlobal);
                SegmentCreated event1 = new SegmentCreated(segment, segmentSize);
                if (event.part == null) {
                    storeWriter.send(event1);
                } else {
                    storeWriter.send(event1, event.part);
                }
            } catch (Throwable e) {
                e.printStackTrace();
                storeWriter.errorOccurred(e);
            }
        }
    }

    private final class PartHandler implements Runnable {

        private final Part part;
        private final int entryPositionLocal;
        private final State entryState;

        PartHandler(Part part, int entryPositionLocal, State entryState) {
            this.part = part;
            this.entryPositionLocal = entryPositionLocal;
            this.entryState = entryState;
        }

        @Override
        public void run() {
            try {
                State state = entryState;
                int positionLocal = entryPositionLocal;
                Segment entryWriteSegment = storeWriter.writeSegment();
                Event sendEvent = null;
                boolean endWritten = false;
                while (true) {
                    if (positionLocal == segmentSize) {
                        sendEvent = new SegmentFull(part);
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
                        storeWriter.writeInt(positionLocal, 0);
                        messageStartSegment = entryWriteSegment;
                        messageStartPositionLocal = positionLocal;
                        positionLocal += LENGTH_BYTES;
                        checksum.reset();
                        contentLength = 0;
                        state = State.WRITTEN_LENGTH;
                    } else if (state == State.WRITTEN_LENGTH) {
                        /////////////////////////////
                        // write padding
                        /////////////////////////////
                        int paddingBytes = 3 - part.length() % 4;
                        storeWriter.writeByte(positionLocal, paddingBytes);
                        for (int i = 1; i <= paddingBytes; i++) {
                            storeWriter.writeByte(positionLocal + i, 0);
                        }
                        positionLocal += paddingBytes + 1;
                        state = State.WRITING_CONTENT;
                    } else if (state == State.WRITING_CONTENT) {
                        if (part instanceof MessageEnd) {
                            state = State.WRITTEN_CONTENT;
                        } else {
                            /////////////////////////////
                            // write content (or continue writing content)
                            /////////////////////////////
                            MessagePart mp = (MessagePart) part;
                            int bytesToWrite = Math.min(segmentSize - positionLocal, mp.bb.remaining());
                            storeWriter.write(positionLocal, mp.bb, bytesToWrite);
                            updateChecksum(checksum, mp.bb, bytesToWrite);
                            positionLocal += bytesToWrite;
                            contentLength += bytesToWrite;
                            if (bytesToWrite != mp.bb.remaining()) {
                                mp.bb.position(mp.bb.position() + bytesToWrite);
                                if (!mp.bb.hasRemaining()) {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    } else if (state == State.WRITTEN_CONTENT) {
                        /////////////////////////////
                        // write checksum
                        /////////////////////////////
                        storeWriter.writeInt(positionLocal, (int) checksum.getValue());
                        positionLocal += CHECKSUM_BYTES;
                        // ensure the length field of the next item is set to zero
                        // if is end of segment then don't need to do it
                        if (positionLocal < segmentSize) {
                            storeWriter.writeInt(positionLocal, 0);
                        }
                        // rewrite the length field at the start of the message
                        storeWriter.writeInt(messageStartSegment, messageStartPositionLocal, contentLength);
                        state = State.FIRST_PART;
                        endWritten = true;
                        break;
                    }
                }
                writePositionGlobal += positionLocal - entryPositionLocal;
                WriteHandler.this.state = state;
                if (sendEvent != null) {
                    storeWriter.send(new SegmentFull(part));
                }
                if (endWritten) {
                    storeWriter.send(new EndWritten());
                    if (messageStartSegment != entryWriteSegment) {
                        storeWriter.closeForWrite(messageStartSegment);
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
                storeWriter.errorOccurred(e);
            }
        }
    }

    // TODO test
    @VisibleForTesting
    static void updateChecksum(Checksum checksum, ByteBuffer bb, int bytesToWrite) {
        if (bb.hasArray()) {
            checksum.update(bb.array(), bb.arrayOffset() + bb.position(), bytesToWrite);
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
