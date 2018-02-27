package org.davidmoten.eq;

import java.io.File;

public final class EmbeddedQueue<T> implements AutoCloseable {

    private final int port;
    private final Serializer<T> serializer;
    private final File directory;
    private final int maxFileSize;
    private final File fileList;

    EmbeddedQueue(int port, Serializer<T> serializer, File directory, int maxFileSize) {
        this.port = port;
        this.serializer = serializer;
        this.directory = directory;
        this.maxFileSize = maxFileSize;
        this.fileList = new File(directory, "file-list.txt");
    }

    void start() {
        // start server port
        
    }

    @Override
    public void close() {
        // shutdown
    }

}
