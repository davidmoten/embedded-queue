package org.davidmoten.eq;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.davidmoten.eq.EmbeddedQueue.Reader;
import org.junit.Test;

public class EmbeddedQueueTest {

    @Test
    public void test() throws IOException {
        Path directory = Files.createTempDirectory(new File("target").toPath(), "test");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EmbeddedQueue q = new EmbeddedQueue(directory.toFile(), 100, 30000, 2);
        Reader reader = q.addReader(0, out);
        reader.start();
        q.addMessage(0, "boo".getBytes());
        q.addMessage(1, "you".getBytes());
        System.out.println(out.toString());
    }

}
