package org.example;

import java.util.List;

public interface BookCleaner {
    String cleanText(String content);

    List<String> readWords(String path);
}
