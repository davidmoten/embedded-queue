package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;

public interface Reader {

    void available();

    void notAvailable();

    void batchFinished();

    long startPositionGlobal();

    void messagePart(ByteBuffer bb);

    void messageFinished();
    
    void messageError(Throwable error);
}
