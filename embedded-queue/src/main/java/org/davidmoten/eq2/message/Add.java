package org.davidmoten.eq2.message;

import java.nio.ByteBuffer;

import io.reactivex.functions.Action;

public class Add implements Event {
    
    final Iterable<ByteBuffer> byteBuffers;
    final Action completionAction;
    
    public Add(Iterable<ByteBuffer> byteBuffers, Action completionAction) {
        this.byteBuffers = byteBuffers;
        this.completionAction = completionAction;
    }
}
