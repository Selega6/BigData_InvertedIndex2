package org.example;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        Controller controller = new Controller(new SparkWebService());
        Runtime.getRuntime().addShutdownHook(new Thread(controller::stop));

        controller.start();
    }
}
