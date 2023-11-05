package org.example;

import java.io.IOException;

public interface DocumentSplitter {
    void splitDocument(String path) throws IOException;
    String prepareMetadata(String text);

    void storeFile(String path, String content, String type);

}
