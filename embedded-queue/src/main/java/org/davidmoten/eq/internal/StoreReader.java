package org.davidmoten.eq.internal;

public interface StoreReader extends HasEventQueue {

    void requestBatch(Reader reader);
    
    void cancel(Reader reader);
}
