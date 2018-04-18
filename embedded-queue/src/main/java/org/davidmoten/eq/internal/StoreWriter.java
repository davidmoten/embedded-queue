package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;

import org.davidmoten.eq.internal.event.Event;

public interface StoreWriter extends HasEventQueue {

    Segment writeSegment();

    void writeInt(int positionLocal, int value);

    void writeByte(int positionLocal, int value);

    void write(int positionLocal, ByteBuffer bb, int length);

    void writeInt(Segment segment, int positionLocal, int value);

    void send(Event event);

    Segment createSegment(long positionGlobal);

    // call from drain thread only
    void addSegment(Segment segment);

    void closeForWrite(Segment writeSegment);
    
    void errorOccurred(Throwable error);

}
