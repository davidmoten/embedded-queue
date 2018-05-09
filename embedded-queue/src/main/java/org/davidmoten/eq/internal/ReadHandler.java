package org.davidmoten.eq.internal;

import java.util.Optional;

import org.davidmoten.eq.internal.FileSystemStore.ReaderState;
import org.davidmoten.eq.internal.event.RequestBatch;

import io.reactivex.Scheduler;

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
            
        }
    }

}
