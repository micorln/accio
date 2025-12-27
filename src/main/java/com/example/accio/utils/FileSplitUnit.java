package com.example.accio.utils;

import java.nio.file.Path;

public class FileSplitUnit {
    
    private Path filePath;

    private long lineNumber;

    private String fileName;

    public FileSplitUnit(Path filePath, long lineNumber, String fileName) {
        this.filePath = filePath;
        this.lineNumber = lineNumber;
        this.fileName = fileName;
    }

    public Path getFilePath() {
        return filePath;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public String getFileName() {
        return fileName;
    }

}
