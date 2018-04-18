package org.davidmoten.eq;

import java.nio.ByteBuffer;

import io.reactivex.Completable;
import io.reactivex.Flowable;

public interface IStore {
    
    Completable add(byte[] bytes);

    Completable add(ByteBuffer bb);

    Completable add(Flowable<ByteBuffer> byteBuffers);
}
