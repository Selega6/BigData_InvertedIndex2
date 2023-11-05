package org.example;

public interface APISource {
    void start();
    APISource startServer();
    void stopServer();
}