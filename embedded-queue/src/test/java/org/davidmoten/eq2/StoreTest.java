package org.davidmoten.eq2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class StoreTest {

    private static final byte[] MSG = "hello".getBytes(StandardCharsets.UTF_8);

    @Test
    public void test() {
        File directory = new File("target/" + System.currentTimeMillis());
        directory.mkdirs();
        int segmentSize = 1024 * 1024;
        Store store = new Store(directory, segmentSize);
        boolean added = store.add(MSG);
        File segment = new File(directory, "0");
        assertTrue(segment.exists());
        assertEquals(segmentSize, segment.length());
        assertTrue(added);
    }

}
