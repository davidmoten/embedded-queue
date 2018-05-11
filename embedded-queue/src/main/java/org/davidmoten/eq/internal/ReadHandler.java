package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.davidmoten.eq.exception.ChecksumFailedException;
import org.davidmoten.eq.exception.NegativeLengthException;
import org.davidmoten.eq.internal.FileSystemStore.ReaderState;
import org.davidmoten.eq.internal.FileSystemStore.ReaderState.State;
import org.davidmoten.eq.internal.event.RequestBatch;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;

/**
 * Ensures that all read events are processed on appropriate schedulers. The
 * StoreReader performs the low-level read operations.
 */
public final class ReadHandler {

    private final StoreReader storeReader;
    private final Scheduler scheduler;
    private final int segmentSize;

    public ReadHandler(StoreReader storeReader, int segmentSize, Scheduler scheduler) {
        this.storeReader = storeReader;
        this.segmentSize = segmentSize;
        this.scheduler = scheduler;
    }

    public void handleRequestBatch(RequestBatch r) {
        ReaderState state = storeReader.state(r.reader);
        Optional<Segment> segment = storeReader.segment(state.readPositionGlobal);
        if (segment.isPresent()) {
            scheduleRead(segment.get(), state);
        }
    }

    private void scheduleRead(Segment segment, ReaderState state) {
        scheduler.scheduleDirect(() -> {
            try {
                int positionLocal = (int) (state.readPositionGlobal - segment.start());
                boolean requestAgain = false;
                while (true) {
                    Preconditions.checkArgument(positionLocal <= segmentSize);
                    if (positionLocal == segmentSize) {
                        requestAgain = true;
                    }
                    if (state.status == State.READING_LENGTH) {
                        int length = segment.readInt(positionLocal);
                        if (length == Delimiter.START) {
                            state.remaining = Delimiter.REMAINING.length;
                            state.status = State.READING_DELIMITER;
                        } else if (length < 0) {
                            // TODO put an event on the queue
                            state.reader.messageError(new NegativeLengthException());
                            break;
                        } else {
                            state.remaining = length;
                            state.status = State.READING_CONTENT;
                        }
                        positionLocal += Constants.LENGTH_BYTES;
                        state.status = State.READING_CONTENT;
                        state.checksum.reset();
                    } else if (state.status == State.READING_CONTENT
                            || state.status == State.READING_DELIMITER) {
                        int bytesToRead = Math.min(state.remaining, segmentSize - positionLocal);
                        ByteBuffer bb = segment.read(positionLocal, bytesToRead);
                        if (state.status == State.READING_CONTENT) {
                            Checksums.updateChecksum(state.checksum, bb, bytesToRead);
                            state.reader.messagePart(bb);
                        }
                        state.remaining -= bytesToRead;
                        positionLocal += bytesToRead;
                        if (state.remaining == 0) {
                            if (state.status == State.READING_CONTENT) {
                                state.status = State.READING_CHECKSUM;
                            } else {
                                // TODO is delimiter, check that value is as expected
                                state.status = State.READING_LENGTH;
                            }
                        }
                    } else if (state.status == State.READING_CHECKSUM) {
                        int checksum = segment.readInt(positionLocal);
                        positionLocal += Constants.CHECKSUM_BYTES;
                        // TODO validate the checksum
                        if (checksum != (int) state.checksum.getValue()) {
                            state.reader.messageError(new ChecksumFailedException());
                        }
                        state.reader.messageFinished();
                        break;
                    }
                }
                state.readPositionGlobal += positionLocal;
                if (requestAgain) {
                    storeReader.requestBatch(state.reader);
                }
            } catch (Throwable e) {
                state.reader.messageError(e);
            }
        });
    }

}
