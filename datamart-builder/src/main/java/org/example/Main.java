package org.example;

import java.io.IOException;
import java.sql.SQLException;
import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        Controller controller = new Controller(new FileDatalake(), new DatabaseWriter());
        controller.start();

    }
}