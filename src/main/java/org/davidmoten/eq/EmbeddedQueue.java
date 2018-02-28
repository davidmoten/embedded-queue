package org.davidmoten.eq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

public final class EmbeddedQueue<T> implements AutoCloseable {

    private final Serializer<T> serializer;
    private final File directory;
    private final int maxFileSize;
    private final File fileList;
    private final String prefix;
    private final Object lock = new Object();
    private final List<File> files;

    EmbeddedQueue(Serializer<T> serializer, File directory, int maxFileSize, String prefix) {
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(directory);
        Preconditions.checkNotNull(prefix);
        Preconditions.checkArgument(maxFileSize >= 0,
                "maxFileSize must be greater than or equal to zero");
        this.serializer = serializer;
        this.directory = directory;
        this.maxFileSize = maxFileSize;
        this.fileList = new File(directory, prefix + "-file-list.txt");
        this.prefix = prefix;
        this.files = loadFiles(fileList);
    }

    private static List<File> loadFiles(File f) {
        List<File> files = new LinkedList<File>();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine())!=null) {
                line = line.trim();
                if (line.length()>0) {
                    files.add(new File(line));
                }
            }
            return files;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void add(byte[] bytes) {
        // add is serialized with itself

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

    @Override
    public void close() {
        // shutdown
    }

}
