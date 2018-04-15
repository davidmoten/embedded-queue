package org.davidmoten.eq.internal;

import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import org.davidmoten.eq.internal.event.MessagePart;
import org.junit.Test;
import org.mockito.Mockito;

import io.reactivex.schedulers.Schedulers;

public class AbstractStoreTest {

    @Test
    public void testCreateSegment() {
        AbstractStore store = Mockito.mock(AbstractStore.class);
        when(store.scheduler()).thenReturn(Schedulers.trampoline());
        Segment segment = Mockito.mock(Segment.class);
        when(store.writeSegment()).thenReturn(segment);
        when(store.segmentSize()).thenReturn(100);
        
        store.handleMessagePart(new MessagePart(ByteBuffer.wrap("hi".getBytes())));
    }

}
