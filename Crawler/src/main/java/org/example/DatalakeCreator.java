package org.example;

import java.io.IOException;
import java.util.Date;

public interface DatalakeCreator {
    void createFolder(String date) throws IOException;
    String setFilePath(String path, Date current, String type);

}
