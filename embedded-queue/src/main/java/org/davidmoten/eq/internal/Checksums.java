package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

final class Checksums {

    private Checksums() {
        // prevent instantiation
    }

    static Checksum create() {
        return new CRC32();
    }
    
 // TODO test
    @VisibleForTesting
    static void updateChecksum(Checksum checksum, ByteBuffer bb, int bytesToWrite) {
        if (bb.hasArray()) {
            checksum.update(bb.array(), bb.arrayOffset() + bb.position(), bytesToWrite);
        } else {
            final int p = bb.position();
            for (int i = 0; i < bytesToWrite; i++) {
                checksum.update(bb.get());
            }
            // revert the position changed by calling bb.get()
            bb.position(p);
        }
    }

}
