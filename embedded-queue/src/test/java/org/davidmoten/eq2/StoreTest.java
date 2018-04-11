package org.davidmoten.eq2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class StoreTest {

    private static final byte[] MSG = "hello".getBytes(StandardCharsets.UTF_8);

    @Test
    public void test() throws IOException {
        File directory = new File("target/" + System.currentTimeMillis());
        directory.mkdirs();
        int segmentSize = 1024 * 1024;
        Store store = new Store(directory, segmentSize);
        boolean added = store.add(MSG);
        File segment = new File(directory, "0");
        assertTrue(segment.exists());
        assertEquals(segmentSize, segment.length());
        assertTrue(added);
        messages(store).stream().forEach(x -> System.out.println(new String(x)));;
    }

    private static List<byte[]> messages(Store store) throws IOException {
        List<byte[]> list = new ArrayList<>();
        int i = -1;
        RandomAccessFile file = null;
        int position = Integer.MAX_VALUE;
        ByteArrayOutputStream bytes = null;
        int bytesToRead = 0;
        while (true) {
            if (file == null ||position >= file.length()) {
                i += 1;
                if (i >= store.segments.size()) {
                    break;
                }
                file = new RandomAccessFile(store.segments.get(i).file, "r");
                position = 0;
                file.seek(position);
            }
            if (bytesToRead == 0) {
                if (bytes != null) {
                    list.add(bytes.toByteArray());
                    bytes.reset();
                } else {
                    bytes = new ByteArrayOutputStream();
                }
                bytesToRead = file.readInt();
            }
            int remaining = (int) (file.length() - 4 - position);
            int n = Math.min(remaining, bytesToRead);
            byte[] b = new byte[n];
            int numRead = file.read(b);
            assert numRead == n;
            bytes.write(b);
            bytesToRead -= n;
            position += 4 + n;
        }
        return list;
    }
}
