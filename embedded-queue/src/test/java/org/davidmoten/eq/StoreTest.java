package org.davidmoten.eq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.davidmoten.eq.IORuntimeException;
import org.davidmoten.eq.Store;
import org.junit.Test;

import io.reactivex.Completable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public class StoreTest {

    private static final byte[] MSG = "hello".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MSG2 = "worldiness".getBytes(StandardCharsets.UTF_8);

    private static final Scheduler ioSync = Schedulers.trampoline();
    private static final Scheduler ioAsync = Schedulers.from(Executors.newFixedThreadPool(10));

    private static int counter = 1;

    @Test
    public void testOneMessageInOneSegmentSynchronous() throws Exception {
        int segmentSize = 50;
        testWriteOneMessage(segmentSize, ioSync);
    }

    @Test
    public void testOneMessageAcrossTwoSegmentsSynchronous() throws Exception {
        int segmentSize = 30;
        testWriteOneMessage(segmentSize, ioSync);
    }
    
    @Test
    public void testOneMessageInOneSegmentAsynchronous() throws Exception {
        int segmentSize = 50;
        testWriteOneMessage(segmentSize, ioAsync);
    }

    @Test
    public void testOneMessageAcrossTwoSegmentsAsynchronous() throws Exception {
        int segmentSize = 30;
        testWriteOneMessage(segmentSize, ioAsync);
    }
    
    @Test
    public void testTwoMessagesInOneSegment() throws Exception {
        int segmentSize = 100;
        File directory = new File("target/" + System.currentTimeMillis() + "_" + (counter++));
        directory.mkdirs();
        Store store = new Store(directory, segmentSize, Schedulers.trampoline());
        assertNull(store.add(MSG).blockingGet());
        assertNull(store.add(MSG2).blockingGet());
        print(store);
        assertEquals(Arrays.asList("hello", "worldiness"), msgs(store));
    }
    
    @Test
    public void testOneLargeMessageSpanningManySegments() throws IOException, NoSuchAlgorithmException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i<=1000;i++) {
            bytes.write(UUID.randomUUID().toString().getBytes());
        }
        byte[] LONG_MESSAGE = bytes.toByteArray();
        int segmentSize = 100;
        File directory = new File("target/" + System.currentTimeMillis() + "_" + (counter++));
        directory.mkdirs();
        Store store = new Store(directory, segmentSize, Schedulers.trampoline());
        assertNull(store.add(LONG_MESSAGE).blockingGet());
        print(store);
        assertEquals(Arrays.asList(new String(LONG_MESSAGE)), msgs(store));
    }

    private static void testWriteOneMessage(int segmentSize, Scheduler io) throws IOException, NoSuchAlgorithmException {
        File directory = new File("target/" + System.currentTimeMillis() + "_" + (counter++));
        directory.mkdirs();
        Store store = new Store(directory, segmentSize, io);
        Completable added = store.add(MSG);
        added.test() //
                .awaitDone(2, TimeUnit.SECONDS) //
                .assertComplete();

        File segment = new File(directory, "0");
        assertTrue(segment.exists());
        assertEquals(segmentSize, segment.length());
        print(store);
        //
        assertEquals(Collections.singletonList("hello"), msgs(store));
    }

    private static void print(Store store) {
        store.segments.stream().forEach(x -> {
            try {
                System.out.println(x.start + ":");
                byte[] bytes = Files.readAllBytes(store.segments.get(0).file.toPath());
                for (byte b : bytes) {
                    System.out.println(b);
                }
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        });
    }

    private static List<String> msgs(Store store) throws NoSuchAlgorithmException, IOException {
        return messages(store) //
                .stream() //
                .map(x -> new String(x, StandardCharsets.UTF_8)) //
                .collect(Collectors.toList());
    }

    private static List<byte[]> messages(Store store) throws IOException, NoSuchAlgorithmException {
        List<byte[]> list = new ArrayList<>();
        int i = -1;
        RandomAccessFile file = null;
        int position = Integer.MAX_VALUE;
        ByteArrayOutputStream bytes = null;
        int bytesToRead = 0;
        boolean readingChecksum = false;
        while (true) {
            if (file == null || position >= file.length()) {
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
                System.out.println("bytesToRead=" + bytesToRead);
                if (bytesToRead == -1) {
                    position = (int) file.length();
                } else {
                    if (bytesToRead == 0) {
                        break;
                    }
                    position += 4;
                }
            }
            int remaining = (int) (file.length() - position);
            int n = Math.min(remaining, bytesToRead);
            byte[] b = new byte[n];
            int numRead = file.read(b);
            assert numRead == n;
            if (!readingChecksum) {
                bytes.write(b);
            }
            bytesToRead -= n;
            if (bytesToRead == 0) {
                if (!readingChecksum) {
                    readingChecksum = true;
                    bytesToRead = store.checksumBytes();
                } else {
                    readingChecksum = false;
                }
            }
            position += n;
        }
        return list;
    }
}
