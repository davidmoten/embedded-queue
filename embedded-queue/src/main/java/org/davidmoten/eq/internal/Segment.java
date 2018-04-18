package org.davidmoten.eq.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.davidmoten.eq.IORuntimeException;

public final class Segment {
    public final File file;
    public final long start;
    private RandomAccessFile writeFile;

    public Segment(File file, long start) {
        this.file = file;
        this.start = start;
    }

    @Override
    public String toString() {
        return "" + start;
    }

    public RandomAccessFile writeFile() {
        if (writeFile == null) {
            try {
                writeFile = new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                throw new IORuntimeException(e);
            }
        }
        return writeFile;
    }

    public void closeForWrite() {
        if (writeFile != null) {
            try {
                writeFile.close();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            } finally {
                writeFile = null;
            }
        }
    }
}
