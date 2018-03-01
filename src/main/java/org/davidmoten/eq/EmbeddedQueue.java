package org.davidmoten.eq;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import com.github.davidmoten.guavamini.Preconditions;

/**
 * Support single producer, multiple consumers, Kafka-style read since timestamp
 * instead of guaranteed delivery (client manages guarantees).
 *
 */
public final class EmbeddedQueue implements AutoCloseable {

    private static final byte[] ZERO_BYTES = toBytes(0);

    private final File directory;
    private final int maxFileSize;
    private final File fileList;
    private final String prefix;
    private int fileNumber = 0;
    private int latestFileSize = 0;
    private final int maxNumFiles;
    // TODO make these specific to a file
    private final Object lengthLock = new Object();
    private final Object indexLock = new Object();
    private final Object filesLock = new Object();
    private final IndexedFiles files;
    private final List<QueueReader> readers = new ArrayList<QueueReader>();

    // mutable
    private RandomAccessFile latestWriteFile;
    private DataOutputStream latestIndexOutputStream;

    EmbeddedQueue(File directory, int maxFileSize, int maxNumFiles, String prefix) {
        Preconditions.checkNotNull(directory);
        Preconditions.checkNotNull(prefix);
        Preconditions.checkArgument(maxFileSize >= 0, "maxFileSize must be greater than or equal to zero");
        this.directory = directory;
        this.maxFileSize = maxFileSize;
        this.fileList = new File(directory, prefix + "-file-list.txt");
        this.prefix = prefix;
        this.files = loadFileList(fileList);
        this.maxNumFiles = maxNumFiles;
    }

    private static IndexedFiles loadFileList(File f) {
        IndexedFiles files = new IndexedFiles();
        if (!f.exists()) {
            return files;
        } else {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.length() > 0) {
                        files.add(new IndexedFile(new File(line), new File(line + ".idx")));
                    }
                }
                return files;
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
    }

    public void add(long time, byte[] message) {
        // add is serialized with itself by client contract

        latestFileSize += message.length + 4 + 8; // + LENGTH + CRC32 checksum
        if (files.first == null || latestFileSize >= maxFileSize) {
            addFile();
        }
        // if no files or latest file size greater than threshold
        // then create new file and put on end of linked list
        // including rewriting the file-list.txt. If new file then
        // add new index file too.

        // with latest file add zero (integer) then bytes then rewrite the zero position
        // with the length of the bytes. The rewriting of the length should be
        // happens-before a read of that length

        try {
            long position = latestWriteFile.getFilePointer();
            // TODO and if crash just after writing zero bytes?
            latestWriteFile.write(message);
            // TODO use ThreadLocal value?
            CRC32 checksum = new CRC32();
            checksum.update(message);
            latestWriteFile.write(toBytes(checksum.getValue()));
            synchronized (lengthLock) {
                latestWriteFile.write(ZERO_BYTES);
            }
            // now go back to the first field of the record (4 bytes = length of message in
            // bytes)
            latestWriteFile.seek(position - 4);
            synchronized (lengthLock) {
                // needs to be happens-before a read so that read
                // doesn't find a partially written length field
                latestWriteFile.write(toBytes(message.length));
            }
            synchronized (indexLock) {
                latestIndexOutputStream.write(toBytes(time));
                latestIndexOutputStream.write(toBytes(position));
            }
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        // TODO use single wip?
        for (QueueReader reader : readers) {
            reader.drain();
        }

    }

    private void addFile() {
        // TODO write -1 to end of current file in length field
        fileNumber++;
        IndexedFile f = createIndexedFile(directory, prefix, fileNumber);
        synchronized (filesLock) {
            files.add(f);
        }
        closeQuietly(latestWriteFile);
        closeQuietly(latestIndexOutputStream);
        try {
            latestWriteFile = new RandomAccessFile(f.file, "rw");
            latestWriteFile.write(ZERO_BYTES);
            latestIndexOutputStream = new DataOutputStream(new FileOutputStream(f.index));
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        latestFileSize = 0;
        // remove old files
        if (files.size() > maxNumFiles) {
            deleteFirst(files);
        }
    }

    private void deleteFirst(IndexedFiles files) {
        // TODO delete expired file
    }

    private static void closeQuietly(RandomAccessFile f) {
        if (f != null) {
            try {
                f.close();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
    }

    private static void closeQuietly(OutputStream out) {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
    }

    private static IndexedFile createIndexedFile(File directory, String prefix, int fileNumber) {
        // prefix with zeroes so alphabetically sorted directory listing shows the list
        // of files in order
        String num = prefixWithZeroes(fileNumber + "", 8);
        File file = new File(directory, prefix + "-" + num);
        File index = new File(directory, prefix + "-" + num + ".idx");
        try {
            file.getParentFile().mkdirs();
            file.createNewFile();
            index.createNewFile();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        return new IndexedFile(file, index);
    }

    private static String prefixWithZeroes(String s, int length) {
        StringBuilder b = new StringBuilder();
        for (int i = s.length(); i <= length; i++) {
            b.append("0");
        }
        b.append(s);
        return b.toString();
    }

    public QueueReader addReader(long sinceTime, OutputStream out) {
        // do binary search on index files to find starting point
        IndexedFileNode node = files.first;
        QueueReader reader = new QueueReader(0, node, out, lengthLock);
        readers.add(reader);
        return reader;
    }

    public void removeReader(QueueReader reader) {

    }

    @Override
    public void close() {
        closeQuietly(latestWriteFile);
        closeQuietly(latestIndexOutputStream);
    }

    private static final class IndexedFile {
        private File file;
        private File index;

        IndexedFile(File file, File index) {
            this.file = file;
            this.index = index;
        }
    }

    private static final byte[] toBytes(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public static byte[] toBytes(long l) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= 8;
        }
        return result;
    }

    public static long toLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    public static int toInt(byte[] bytes) {
        int ret = 0;
        for (int i = 0; i < 4 && i < bytes.length; i++) {
            ret <<= 8;
            ret |= (int) bytes[i] & 0xFF;
        }
        return ret;
    }

    public static final class QueueReader {
        private IndexedFileNode current;
        // requests can come from other threads so needs to be atomic
        private final AtomicLong requested = new AtomicLong();

        // synchronized by wip
        private long emitted;
        private volatile boolean cancelled = false;
        private final OutputStream out;
        private final Object lengthLock;

        private final AtomicInteger wip = new AtomicInteger();

        // mutable
        private RandomAccessFile file;

        QueueReader(int startPosition, IndexedFileNode current, OutputStream out, Object lengthLock) {
            this.current = current;
            this.out = out;
            this.lengthLock = lengthLock;
            try {
                this.file = new RandomAccessFile(current.file.file, "rw");
                file.seek(startPosition);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void request(long n) {
            // CAS loop to add request and avoid overflow
            while (true) {
                long r = requested.get();
                if (r == Long.MAX_VALUE) {
                    return;
                } else {
                    long r2 = r + n;
                    if (r2 < 0) {
                        r2 = Long.MAX_VALUE;
                    }
                    if (requested.compareAndSet(r, r2)) {
                        break;
                    }
                }
            }

            drain();
        }

        public void cancel() {
            cancelled = true;
            closeQuietly(out);
        }

        private final byte[] lengthBytes = new byte[4];
        private final byte[] crcBytes = new byte[4];

        // mutable, persistent if message length does not vary
        private byte[] messageBytes = new byte[0];

        private void drain() {
            // serialize drain loop using wip atomic counter
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = emitted;
                    while (e != r) {
                        if (cancelled) {
                            return;
                        }
                        try {
                            if (file == null) {
                                break;
                            }
                            long position = file.getFilePointer();
                            if (file.length() - position >= 4) {

                                // TODO make non-blocking
                                synchronized (lengthLock) {
                                    file.readFully(lengthBytes);
                                }
                                int length = toInt(lengthBytes);
                                if (length == 0) {
                                    file.seek(position);
                                    break;
                                }
                                if (length == -1) {
                                    // EOF
                                    file.close();
                                } else {
                                    if (messageBytes.length != length) {
                                        messageBytes = new byte[length];
                                    }
                                    file.readFully(messageBytes);
                                    file.readFully(crcBytes);
                                    // TODO use ThreadLocal cache for CRC32
                                    CRC32 crc = new CRC32();
                                    crc.update(messageBytes);
                                    if (crc.getValue() != toLong(crcBytes)) {
                                        // TODO handle corrupt message (report it, alert, archive it)
                                        System.err.println("message had invalid checksum so was discarded");
                                    } else {
                                        // write message to output stream
                                        // don't include crc because protocol will ensure no corruption
                                        out.write(lengthBytes);
                                        out.write(messageBytes);
                                    }
                                }
                            }
                        } catch (IOException ex) {
                            throw new IORuntimeException(ex);
                        }

                        // go to read point
                        // if end-of-file or zero length found then break
                        // else read length
                        // stream bytes to that length
                        e++;
                    }
                    emitted = e;
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            }
        }
    }

    private static final class IndexedFileNode {
        final IndexedFile file;
        IndexedFileNode next;

        IndexedFileNode(IndexedFile file, IndexedFileNode next) {
            this.file = file;
            this.next = next;
        }
    }

    // Not thread-safe
    private static final class IndexedFiles {
        // mutable
        IndexedFileNode first;
        IndexedFileNode last;
        int size;

        public void add(IndexedFile f) {
            if (first == null) {
                first = new IndexedFileNode(f, null);
                last = first;
            } else {
                IndexedFileNode node = new IndexedFileNode(f, null);
                last.next = node;
            }
            size++;
        }

        public int size() {
            return size;
        }
    }

}
