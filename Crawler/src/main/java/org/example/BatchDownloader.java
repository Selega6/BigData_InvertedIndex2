package org.example;

import java.io.File;

import static java.lang.Thread.sleep;

public class BatchDownloader {
    int batchSize;
    GutenbergFileReader gutenbergFileReader;
    String[] bookIds;

    public BatchDownloader(int batchSize, GutenbergFileReader gutenbergFileReader, String[] bookIds) {
        this.batchSize = batchSize;
        this.gutenbergFileReader = gutenbergFileReader;
        this.bookIds = bookIds;
    }

    public void download() {
        int count = 0;
        setBooksBatch(0);

        while (count == 0) {
            for (int i = 0; i < batchSize; i++) {
                try {
                    gutenbergFileReader.downloadFile(bookIds[i]);
                    sleep(18000);
                    count++;
                } catch (Exception e) {
                    System.out.println("Error downloading file: " + bookIds[i]);
                }
            }

            if (count == 0) {
                setBooksBatch(10);
            }
        }
    }

    public void setBooksBatch(int mod) {
        int start = getLastBookId("datalake");
        int count = 0;
        for (int i = start+1+mod; i < start+1+mod+batchSize; i++) {
            bookIds[count] = URLSetter(i);
            count++;
        }
    }
    public int getLastBookId(String baseDirectory) {
        File datalake = new File(baseDirectory);

        int lastBookId = 0;

        File[] dateDirectories = datalake.listFiles(File::isDirectory);

        if (dateDirectories != null) {
            for (File dateDir : dateDirectories) {
                File[] bookDirectories = dateDir.listFiles(File::isDirectory);

                if (bookDirectories != null && bookDirectories.length > 0) {
                    for (File bookDir : bookDirectories) {
                        int extractedId = extractIdFromDirectoryName(bookDir.getName());
                        lastBookId = Math.max(lastBookId, extractedId);
                    }
                }
            }
        }
        return lastBookId;
    }

    private int extractIdFromDirectoryName(String directoryName) {
        int id;
        try {
            id = Integer.parseInt(directoryName);
        } catch (NumberFormatException e) {
            id = 0;
        }
        return id;
    }

    public String URLSetter(int id) {
        return "https://www.gutenberg.org/cache/epub/" + id + "/pg" + id + ".txt";
    }
}
