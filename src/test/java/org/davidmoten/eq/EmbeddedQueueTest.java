package org.davidmoten.eq;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.davidmoten.eq.EmbeddedQueue.Reader;
import org.junit.Test;

public class EmbeddedQueueTest {

    @Test
    public void testOneSegmentWriteAndRead() throws IOException, InterruptedException {
        Path directory = Files.createTempDirectory(new File("target").toPath(), "test");
        SynchronizedOutputStream out = new SynchronizedOutputStream();
        EmbeddedQueue q = EmbeddedQueue //
                .directory(directory.toFile()) //
                .maxSegmentSize(100) //
                .addSegmentMaxWaitTime(30, TimeUnit.SECONDS) //
                .messageBufferSize(8192) //
                .build();
        Reader reader = q.readFromOffset(0, out);
        reader.start();
        q.addMessage(0, "boo".getBytes());
        q.addMessage(1, "you".getBytes());
        reader.request(5);
        Thread.sleep(500);
        assertEquals(30, out.bytes().length);
        assertEquals(Arrays.asList("boo", "you"), messages(out.bytes()));
    }

    @Test
    public void testWriteReadFromOffsetOneSegment() throws IOException, InterruptedException {
        Path directory = Files.createTempDirectory(new File("target").toPath(), "test");
        SynchronizedOutputStream out = new SynchronizedOutputStream();
        EmbeddedQueue q = EmbeddedQueue //
                .directory(directory.toFile()) //
                .maxSegmentSize(100) //
                .addSegmentMaxWaitTime(30, TimeUnit.SECONDS) //
                .messageBufferSize(8192) //
                .build();
        Reader reader = q.readFromOffset("boo".getBytes().length + q.inputHeaderLength(), out);
        reader.start();
        q.addMessage(0, "boo".getBytes());
        q.addMessage(1, "you".getBytes());
        reader.request(5);
        Thread.sleep(500);
        assertEquals(15, out.bytes().length);
        assertEquals(Arrays.asList("you"), messages(out.bytes()));
    }

    @Test
    public void testMultipleSegments() throws IOException, InterruptedException {
        Path directory = Files.createTempDirectory(new File("target").toPath(), "test");
        SynchronizedOutputStream out = new SynchronizedOutputStream();
        EmbeddedQueue q = EmbeddedQueue //
                .directory(directory.toFile()) //
                .maxSegmentSize(5) //
                .addSegmentMaxWaitTime(30, TimeUnit.SECONDS) //
                .messageBufferSize(8192) //
                .build();
        Reader reader = q.readFromOffset(0, out);
        reader.start();
        q.addMessage(0, "boo".getBytes());
        q.addMessage(1, "you".getBytes());
        reader.request(5);
        Thread.sleep(500);
        assertEquals(Arrays.asList("boo", "you"), messages(out.bytes()));
        assertEquals(30, out.bytes().length);
    }

    private static List<String> messages(byte[] bytes) throws IOException {
        List<String> list = new ArrayList<>();
        DataInputStream d = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            while (true) {
                int length = d.readInt();
                // read offset
                d.readLong();
                byte[] b = new byte[length];
                d.readFully(b);
                list.add(new String(b));
            }
        } catch (EOFException e) {
            return list;
        }
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
