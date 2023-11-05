package org.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class MissingFilesManagement {
    private final GutenbergSplitter splitter;

    public MissingFilesManagement() {
        this.splitter = new GutenbergSplitter();
    }

    public void splitMissingFiles() throws IOException {
        GutenbergFileReader gutenbergFileReader = new GutenbergFileReader();
        String[] missingFiles = getMissingFiles();
        for (String missingFile : missingFiles) {
            int lastBackslashIndex = missingFile.lastIndexOf('\\');
            missingFile = missingFile.substring(lastBackslashIndex + 1);
            splitter.splitDocument(missingFile);
            splitter.storeFile(missingFile, gutenbergFileReader.read(missingFile, "raw"), "raw");

            Date currentDate = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            String yesterdayDate = dateFormat.format(new Date(currentDate.getTime() - 1000L * 60L * 60L * 24L));
            Path directoryToRemove = Paths.get("datalake", yesterdayDate, missingFile);
            removeDirectory(directoryToRemove);
        }

    }

    private void removeDirectory(Path directoryPath) {
        try {
            if (Files.exists(directoryPath)) {
                Files.walk(directoryPath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] getMissingFiles() {
        String[] missingFiles = new String[100];
        try {
            Date yesterday = new Date(System.currentTimeMillis() - 1000L * 60L * 60L * 24L);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            Path datalakeFolder = Paths.get("datalake", dateFormat.format(yesterday));

            DirectoryStream<Path> dateDirectories = Files.newDirectoryStream(datalakeFolder);
            int i = 0;
            for (Path dateDirectory : dateDirectories) {
                int count = 0;
                String fileName = "";
                if (Files.isDirectory(dateDirectory)) {
                    DirectoryStream<Path> bookDirectories = Files.newDirectoryStream(dateDirectory);
                    for (Path bookDirectory : bookDirectories) {
                        if (Files.isDirectory(bookDirectory)) {
                            DirectoryStream<Path> contentFiles = Files.newDirectoryStream(bookDirectory);

                            for (Path file : contentFiles) {
                                fileName = file.toString();
                                fileName = fileName.substring(0, fileName.length() - 4);
                                count++;
                            }
                        }
                    }
                }
                if (count < 3) {
                    System.out.println(fileName);
                    missingFiles[i] = fileName;
                    i++;
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        List<String> validFiles = new ArrayList<>();
        for (String elemento : missingFiles) {
            if (elemento != null && !elemento.isEmpty()) {
                validFiles.add(elemento);
            }
        }

        String[] filteredFiles = validFiles.toArray(new String[0]);

        return filteredFiles;
    }
}