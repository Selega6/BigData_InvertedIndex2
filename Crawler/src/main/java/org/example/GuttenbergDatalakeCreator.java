package org.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GuttenbergDatalakeCreator implements DatalakeCreator {
    @Override
    public void createFolder(String folderPath) throws IOException {
        Files.createDirectories(Path.of(folderPath + "//raw"));
        Files.createDirectories(Path.of(folderPath + "//content"));
        Files.createDirectories(Path.of(folderPath + "//metadata"));
    }

    @Override
    public String setFilePath(String filename, Date currentDate, String type) {

        String filePath;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            String folderPath = "datalake//" + dateFormat.format(currentDate) + "//" + filename;

            if (!Files.isDirectory(Path.of(folderPath))) {
                createFolder(folderPath);
            }

            filePath = folderPath + "//" + type + "//";

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return filePath;
    }
    public void createDateFolder(Date currentDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String folderPath = "datalake//" + dateFormat.format(currentDate);
        if (!Files.isDirectory(Path.of(folderPath))) {
            new File("/path/directory").mkdirs();
        }
    }
}

