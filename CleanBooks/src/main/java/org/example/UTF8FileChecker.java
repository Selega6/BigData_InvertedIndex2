package org.example;

import java.io.*;

public class UTF8FileChecker {
    public boolean isUTF8(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            if (fis.available() >= 3) {
                byte[] bom = new byte[3];
                fis.read(bom);
                return bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}