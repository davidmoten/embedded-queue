package org.davidmoten.eq.internal.event;

import org.davidmoten.eq.internal.Reader;

public final class BatchFinished implements Event {

    public final Reader reader;

    public BatchFinished(Reader reader) {
        this.reader = reader;
    }

}
