package org.davidmoten.eq.internal;

import java.util.Optional;

import org.davidmoten.eq.internal.FileSystemStore.ReaderState;

/**
 * Performs low level read operations. Serialization and scheduling is handled
 * by the Store, ReadHandler.
 */
public interface StoreReader extends HasEventQueue {

    void requestBatch(Reader reader);

    void cancel(Reader reader);

    ReaderState state(Reader reader);

    Optional<Segment> segment(long readPositionGlobal);
}
