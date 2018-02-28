package org.davidmoten.eq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

public final class EmbeddedQueue<T> implements AutoCloseable {

    private final File directory;
    private final int maxFileSize;
    private final File fileList;
    private final String prefix;
    private final List<IndexedFile> files;
    private int fileNumber = 0;
    private int latestFileSize = 0;
    private final int maxNumFiles;

    //mutable
    private OutputStream out;
    private OutputStream outIndex;

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

    private static List<IndexedFile> loadFileList(File f) {
        List<IndexedFile> files = new LinkedList<IndexedFile>();
        if (!f.exists()) {
            return files;
        }
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

    public void add(long time, byte[] bytes) {
        // add is serialized with itself

        if (files.isEmpty() || latestFileSize + bytes.length >= maxFileSize) {
            addFile();
        }
        // if no files or latest file size greater than threshold
        // then create new file and put on end of linked list
        // including rewriting the file-list.txt. If new file then
        // add new index file too.

        // with latest file add zero (integer) then bytes then rewrite the zero position
        // with the length of the bytes. The rewriting of the length should be
        // happens-before a read of that length

        // add the time and position to the latest index file. The addition should be
        // happens-before a read of that length.
    }

    private void addFile() {
        fileNumber++;
        files.add(createIndexedFile(directory, prefix, fileNumber));
        closeQuietly(out);
        closeQuietly(outIndex);
        IndexedFile last = files.get(files.size() - 1);
        try {
            out = new FileOutputStream(last.file);
            outIndex = new FileOutputStream(last.index);
        } catch (FileNotFoundException e) {
            throw new IORuntimeException(e);
        }
        latestFileSize = 0;
        // remove old files
        if (files.size() > maxNumFiles) {

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
        // shutdown
    }

    private static final class IndexedFile {
        private File file;
        private File index;

        IndexedFile(File file, File index) {
            this.file = file;
            this.index = index;
        }
    }

}
