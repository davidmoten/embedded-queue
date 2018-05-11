package org.davidmoten.eq.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.davidmoten.eq.IORuntimeException;

public final class Segment {

    private final File file;
    private final long start;
    private RandomAccessFile raf;

    public Segment(File file, long start) {
        this.file = file;
        this.start = start;
    }

    public long start() {
        return start;
    }

    @Override
    public String toString() {
        return "" + start;
    }

    private RandomAccessFile raf() {
        if (raf == null) {
            try {
                raf = new RandomAccessFile(file, "rw");
            } catch (final FileNotFoundException e) {
                throw new IORuntimeException(e);
            }
        }
        return raf;
    }

    public void createFile(long length) {
        try {
            raf().setLength(length);
        } catch (final IOException e) {
            throw new IORuntimeException(e);
        }
    }

    public void writeInt(int positionLocal, int value) {
        final RandomAccessFile f = raf();
        try {
            f.seek(positionLocal);
            f.writeInt(value);
        } catch (final IOException e) {
            throw new IORuntimeException(e);
        }
    }

    public void writeByte(int positionLocal, int value) {
        final RandomAccessFile f = raf();
        try {
            f.seek(positionLocal);
            f.write(value);
        } catch (final IOException e) {
            throw new IORuntimeException(e);
        }

    }

    public void write(int positionLocal, ByteBuffer bb, int length) {
        final RandomAccessFile f = raf();
        try {
            f.seek(positionLocal);
            if (bb.hasArray()) {
                f.write(bb.array(), bb.arrayOffset() + bb.position(), length);
            } else {
                for (int i = 0; i < length; i++) {
                    f.write(bb.get());
                }
            }
        } catch (final IOException e) {
            throw new IORuntimeException(e);
        }

    }

    public void writeInt(Segment segment, int positionLocal, int value) {
        final RandomAccessFile f = raf();
        try {
            f.seek(positionLocal);
            f.writeInt(value);
        } catch (final IOException e) {
            throw new IORuntimeException(e);
        }

    }

    public void closeForWrite() {
        if (raf != null) {
            try {
                raf.close();
            } catch (final IOException e) {
                throw new IORuntimeException(e);
            } finally {
                raf = null;
            }
        }
    }

    public File file() {
        return file;
    }

    public int readInt(long positionLocal) {
        try {
            raf.seek(positionLocal);
            return raf.readInt();
        } catch (final IOException e) {
            throw new IORuntimeException(e);
        }
    }

    public ByteBuffer read(int positionLocal, int length) {
        byte[] b = new byte[length];
        try {
            raf.seek(positionLocal);
            raf.read(b, 0, length);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        return ByteBuffer.wrap(b);
    }

}
