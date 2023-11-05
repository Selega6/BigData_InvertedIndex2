package org.example;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args){
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Controller controller = new Controller();
        executorService.execute(controller::scheduleMissingFilesCheck);
        controller.datalakeChecker();
        controller.checkTodayFolderAndExecute();
    }

}
