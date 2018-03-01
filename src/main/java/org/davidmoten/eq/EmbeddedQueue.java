package org.davidmoten.eq;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

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

    public void add(long time, byte[] bytes) {
        // add is serialized with itself

        if (files == null || latestFileSize + bytes.length >= maxFileSize) {
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
            latestWriteFile.write(ZERO_BYTES);
            latestWriteFile.write(bytes);
            latestWriteFile.seek(position);
            synchronized (lengthLock) {
                // needs to be happens-before a read so that read
                // doesn't find a partially written length field
                latestWriteFile.write(toBytes(bytes.length));
            }
            synchronized (indexLock) {
                latestIndexOutputStream.write(toBytes(time));
                latestIndexOutputStream.write(toBytes(position));
            }
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

    }

    private void addFile() {
        fileNumber++;
        IndexedFile f = createIndexedFile(directory, prefix, fileNumber);
        synchronized (filesLock) {
            files.add(f);
        }
        closeQuietly(latestWriteFile);
        closeQuietly(latestIndexOutputStream);
        IndexedFile last = files.last.file;
        try {
            latestWriteFile = new RandomAccessFile(last.file, "rw");
            latestIndexOutputStream = new DataOutputStream(new FileOutputStream(last.index));
        } catch (FileNotFoundException e) {
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

    private static final class QueueReaders {

    }

    private static final class QueueReader {
        final long sinceTime;
        IndexedFileNode current;

        QueueReader(long sinceTime, IndexedFileNode current) {
            this.sinceTime = sinceTime;
            this.current = current;
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

    //Not thread-safe
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
