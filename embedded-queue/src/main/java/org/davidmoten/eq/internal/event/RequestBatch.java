package org.davidmoten.eq.internal.event;

import org.davidmoten.eq.internal.Reader;

public final class RequestBatch implements Event {

    public final Reader reader;

    public RequestBatch(Reader reader) {
        this.reader = reader;
    }
}
