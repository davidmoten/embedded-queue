package org.davidmoten.eq.internal;

import java.util.Optional;

import org.davidmoten.eq.internal.FileSystemStore.ReaderState;

public interface StoreReader extends HasEventQueue {

    void requestBatch(Reader reader);
    
    void cancel(Reader reader);

    ReaderState state(Reader reader);

    Optional<Segment> segment(long readPositionGlobal);
}
