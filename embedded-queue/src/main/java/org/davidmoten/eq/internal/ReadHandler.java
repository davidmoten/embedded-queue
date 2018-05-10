package org.davidmoten.eq.internal;

import java.util.Optional;

import org.davidmoten.eq.internal.FileSystemStore.ReaderState;
import org.davidmoten.eq.internal.event.RequestBatch;

import io.reactivex.Scheduler;

/**
 * Ensures that all read events are processed on appropriate schedulers.
 * The StoreReader performs the low-level read operations.
 */
public final class ReadHandler {

    private final StoreReader storeReader;
    private final Scheduler scheduler;

    public ReadHandler(StoreReader storeReader, Scheduler scheduler) {
        this.storeReader = storeReader;
        this.scheduler = scheduler;
    }

    public void handleRequestBatch(RequestBatch r) {
        ReaderState state = storeReader.state(r.reader);
        Optional<Segment> segment = storeReader.segment(state.readPositionGlobal);
        if (segment.isPresent()) {
            scheduler.scheduleDirect(() -> {
                storeReader.requestBatch(r.reader);
            });
        }
    }

}
