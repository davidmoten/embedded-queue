package org.davidmoten.eq.internal.event;

import org.davidmoten.eq.internal.Reader;
import org.davidmoten.eq.internal.event.Event;

public final class CancelReader implements Event {

    final Reader reader;

    public CancelReader(Reader reader) {
        this.reader = reader;
    }

}
