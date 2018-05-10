package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Delimiter {

    public static final byte[] BYTES = new byte[] { -50, -58, -23, 65, 34, 72, -77, 47, 1, -8, 54, 119, 18, 47, 33,
            -111, -4, -10, -14, -32, 99, -105, 70, -68, -48, 8, -54, 16, -117, -77, 109, 102, 43, 87, 31, -42, -107,
            -61, -15, -95, 127, -27, 44, 59, 34, 45, 19, 64, 109, -123, 21, -86, -98, -10, -45, -61, -96, -67, 50, -59,
            80, -128, 15, -68 };

    public static final byte[] BYTES_WITH_ZERO = Arrays.copyOfRange(BYTES, 0, BYTES.length + 4);

    public static final int START;
    public static final byte[] REMAINING;

    static {
        final ByteBuffer bb = ByteBuffer.wrap(BYTES);
        START = bb.getInt();
        REMAINING = Arrays.copyOfRange(BYTES, 4, BYTES.length);
    }

}
