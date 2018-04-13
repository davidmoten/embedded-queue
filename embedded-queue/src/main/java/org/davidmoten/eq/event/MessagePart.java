package org.davidmoten.eq.event;

import java.nio.ByteBuffer;

public class MessagePart implements Part {

    public final ByteBuffer bb;

    public MessagePart(ByteBuffer bb) {
        this.bb = bb;
    }
}
