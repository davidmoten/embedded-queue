package org.davidmoten.eq;

import java.io.IOException;

public final class IORuntimeException extends RuntimeException {

    private static final long serialVersionUID = -4225528932807442025L;

    public IORuntimeException(IOException e) {
        super(e);
    }
}
