package org.example;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;

public class GutenbergFileReader implements BookReader {

    @Override
    public void downloadFile(String fileURL) throws IOException {
        URL url = new URL(fileURL);
        URLConnection connection = url.openConnection();

        try (InputStream inputStream = connection.getInputStream()) {
            String fileName = fileURL.substring(fileURL.lastIndexOf('/') + 1);
            fileName = fileName.substring(2);
            String bookDirectory = fileName.substring(0, fileName.length() - 4);
            String downloadPath = new GuttenbergDatalakeCreator().setFilePath(bookDirectory, new Date(), "raw");
            String fullPath = downloadPath + fileName;

            try (OutputStream outputStream = new FileOutputStream(fullPath)) {
                byte[] buffer = new byte[16384];
                int length;

                while ((length = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, length);
                }
            }
        }
    }

    @Override
    public String read(String bookID, String type) throws IOException {
        String path = getFilePath(bookID, type);
        File file = new File(path);
        if (file.exists()) {
            StringBuilder content = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }
            }
            return content.toString();
        } else {
            throw new FileNotFoundException("File not found: " + path);
        }
    }
    public String getFilePath(String bookId, String type) {
        String baseDirectory = "datalake";
        File baseDir = new File(baseDirectory);
        File[] dateDirectories = baseDir.listFiles(File::isDirectory);
        if (dateDirectories != null) {
            for (File dateDirectory : dateDirectories) {
                File bookFile = new File(dateDirectory,bookId + "\\"+ type + "\\" + bookId + ".txt");
                if (bookFile.exists()) {
                    return bookFile.getAbsolutePath();
                }
            }
        }
        return "File not found";
    }
}
