package org.davidmoten.eq.internal;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DelimiterTest {

    
    @Test
    public void test() {
        assertEquals(Delimiter.START, -825824959);
        assertEquals(64, Delimiter.BYTES.length);
        assertEquals(60, Delimiter.REMAINING.length);
        assertEquals(Delimiter.BYTES[4], Delimiter.REMAINING[0]);
    }
}
