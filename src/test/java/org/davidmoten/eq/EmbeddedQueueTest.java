package org.davidmoten.eq;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.davidmoten.eq.EmbeddedQueue.Reader;
import org.junit.Test;

public class EmbeddedQueueTest {

    @Test
    public void test() throws IOException, InterruptedException {
        Path directory = Files.createTempDirectory(new File("target").toPath(), "test");
        SynchronizedOutputStream out = new SynchronizedOutputStream();
        EmbeddedQueue q = new EmbeddedQueue(directory.toFile(), 100, 30000, 2);
        Reader reader = q.addReader(0, out);
        reader.start();
        q.addMessage(0, "boo".getBytes());
        q.addMessage(1, "you".getBytes());
        reader.request(5);
        Thread.sleep(500);
        assertEquals(14, out.bytes().length);
    }

    private static final class SynchronizedOutputStream extends OutputStream {

        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        @Override
        public void write(int b) throws IOException {
            synchronized (this) {
                bytes.write(b);
            }
        }

        byte[] bytes() {
            synchronized (this) {
                return bytes.toByteArray();
            }
        }

    }

}
