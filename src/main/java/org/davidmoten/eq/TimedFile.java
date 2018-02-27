package org.davidmoten.eq;

import java.io.File;

class TimedFile {

    final long time;
    final File file;

    TimedFile(long time, File file) {
        this.time = time;
        this.file = file;
    }

}
