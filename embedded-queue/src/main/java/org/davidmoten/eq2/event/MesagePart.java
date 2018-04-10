package org.davidmoten.eq2.event;

import java.nio.ByteBuffer;

public class MesagePart implements Event {
    
    public final ByteBuffer bb;

    public MesagePart(ByteBuffer bb) {
        this.bb = bb;
    }
}
