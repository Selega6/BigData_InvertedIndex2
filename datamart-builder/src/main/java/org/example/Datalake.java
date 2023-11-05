package org.example;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public interface Datalake {
    Map<Book, Map<Associate, Word>> read(File directory, int wordMaxId) throws IOException;

}
