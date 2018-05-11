package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.Optional;

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
                        state.remaining = segment.readInt(positionLocal);
                        positionLocal += Constants.LENGTH_BYTES;
                        state.status = State.READING_CONTENT;
                    } else if (state.status == State.READING_CONTENT) {
                        int bytesToRead = Math.min(state.remaining, segmentSize - positionLocal);
                        ByteBuffer bb = segment.read(positionLocal, bytesToRead);
                        state.reader.messagePart(bb);
                        state.remaining -= bytesToRead;
                        positionLocal += bytesToRead;
                        if (state.remaining == 0) {
                            state.status = State.READING_CHECKSUM;
                        }
                    } else if (state.status == State.READING_CHECKSUM) {
                        int checksum = segment.readInt(positionLocal);
                        positionLocal += Constants.CHECKSUM_BYTES;
                        // TODO validate the checksum
                        assert checksum != Integer.MAX_VALUE;
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
