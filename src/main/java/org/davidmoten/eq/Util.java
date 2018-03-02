package org.davidmoten.eq;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

class Util {

    private Util() {
        // prevent instantiation
    }

    public static final byte[] toBytes(int value) {
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

    public static void closeQuietly(RandomAccessFile f) {
        if (f != null) {
            try {
                f.close();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
    }

    public static void closeQuietly(OutputStream out) {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
    }

    public static String prefixWithZeroes(String s, int length) {
        StringBuilder b = new StringBuilder();
        for (int i = s.length(); i <= length; i++) {
            b.append("0");
        }
        b.append(s);
        return b.toString();
    }
    
}
