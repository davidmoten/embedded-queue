package org.davidmoten.eq;

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
import java.util.stream.Collectors;

import org.davidmoten.eq.EmbeddedQueue.Reader;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedQueueTest {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedQueueTest.class);

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
        waitForMessages(out, "boo", "you");
        reader.cancel();
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
        waitForMessages(out, "you");
    }

    @Test
    public void testMultipleSegments() throws IOException, InterruptedException {
        for (int i = 0; i < 1000; i++) {
            log.info("=========================");
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
            waitForMessages(out, "boo", "you");
            reader.cancel();
            Arrays.stream(directory.toFile().listFiles()).forEach(f -> f.delete());
            directory.toFile().delete();
        }
    }

    private void waitForMessages(SynchronizedOutputStream out, String... messages)
            throws InterruptedException, IOException {
        boolean ok = false;
        List<String> list = Arrays.stream(messages).collect(Collectors.toList());
        List<String> msgs = null;
        for (int j = 0; j < 50; j++) {
            Thread.sleep(10);
            msgs = messages(out.bytes());
            if (list.equals(msgs)) {
                ok = true;
                break;
            }
        }
        if (!ok) {
            Assert.fail("expected " + list + " was " + msgs);
        }
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
