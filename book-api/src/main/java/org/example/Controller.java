package org.example;

import java.sql.SQLException;

public class Controller {
    private final APISource api;
    private final SqliteReader dbReader;

    public Controller(APISource api) throws SQLException {
        this.api = api;
        this.dbReader = new SqliteReader();
    }

    public void start() {
        api.startServer();
        api.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }


    public void stop() {
        try {
            api.stopServer();
        } finally {
            try {
                dbReader.closeConnection();
            } catch (SQLException e) {
                System.err.println("Error closing the database connection: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
