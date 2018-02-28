package org.davidmoten.eq;

import java.io.File;

import com.github.davidmoten.guavamini.Preconditions;

public final class EmbeddedQueue<T> implements AutoCloseable {

    private final Serializer<T> serializer;
    private final File directory;
    private final int maxFileSize;
    private final File fileList;
    private final String prefix;
    private final Object lock = new Object();

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
