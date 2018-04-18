package org.davidmoten.eq;

import java.nio.ByteBuffer;

import io.reactivex.Completable;
import io.reactivex.Flowable;

public interface Store {
    
    Completable add(byte[] bytes);

    Completable add(ByteBuffer bb);

    Completable add(Flowable<ByteBuffer> byteBuffers);
    
    Flowable<Flowable<ByteBuffer>> read(long positionGlobal);
}
