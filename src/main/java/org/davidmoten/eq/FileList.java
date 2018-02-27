package org.davidmoten.eq;

import java.io.File;
import java.util.List;

public class FileList {

    // has the list of objectFiles
    private File file;
    private List<File> objectFiles;
    private List<File> timeFiles;

    FileList() {
        this.file = file;
    }

    // each filename returned is a long being the timestamp of the first record
    List<File> objectFiles() {
        return objectFiles;
    }

    // each file has time and position in the file of next record
    // same name as object file but with `.idx` suffix
    // fixed length records
    List<File> timeFiles() {
        return timeFiles;
    }

    void checkExpiry() {

    }

}
