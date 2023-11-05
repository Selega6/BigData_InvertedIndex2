package org.example;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.Thread.sleep;

public class Controller {
    GutenbergFileReader gutenbergFileReader;
    BatchDownloader batchDownloader;
    int batchsize;
    String[] ids;
    GuttenbergDatalakeCreator guttenbergDatalakeCreator;
    public Controller(int batchsize) {
        this.gutenbergFileReader = new GutenbergFileReader();
        this.batchsize = batchsize;
        this.ids = new String[batchsize];
        this.batchDownloader = new BatchDownloader(batchsize, gutenbergFileReader, ids);
        this.guttenbergDatalakeCreator = new GuttenbergDatalakeCreator();
    }

    public void execute() throws IOException, InterruptedException {
        Date current = new Date();
        guttenbergDatalakeCreator.createDateFolder(current);
        sleep(1000);
        batchDownloader.download();
    }

    public void run() {
        Timer timer = new Timer();
        int delay = 0;
        int period = 120000;

        TimerTask task = new TimerTask() {
            public void run() {
                try {
                    execute();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        timer.scheduleAtFixedRate(task, delay, period);
    }
}