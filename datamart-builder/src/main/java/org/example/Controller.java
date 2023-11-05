package org.example;

import java.io.IOException;
import java.nio.file.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Controller {
    private final Datalake datalake;
    private final Datamart datamart;

    public Controller(Datalake datalake, Datamart datamart) {
        this.datalake = datalake;
        this.datamart = datamart;
    }

    public void start() throws SQLException, IOException, InterruptedException {
        System.out.println("Watching Datalake Directory");
        Date currentDate = new Date();
        WatchService watchService = FileSystems.getDefault().newWatchService();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String folderPath = "datalake/" + dateFormat.format(currentDate);
        Path path = Paths.get(folderPath);
        while (!Files.exists(path) || !Files.isDirectory(path)) {
            Thread.sleep(10);
        }
        path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

        WatchKey key;
        while (true) {
            try {
                key = watchService.take();
            } catch (InterruptedException ex) {
                return;
            }
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();
                    System.out.println(kind + ": " + filename);
                    Path totalPath = Paths.get(folderPath + "/" + filename);
                    watchBookFolder(totalPath);
                }
            }
            boolean valid = key.reset();
            if (!valid) {
                break;
            }
        }
    }

    private void watchBookFolder(Path bookFolder) throws IOException, InterruptedException, SQLException {
        System.out.println("Watching Book Directory: " + bookFolder);
        Path contentPath = bookFolder.resolve("content");

        while (!Files.exists(contentPath) || !Files.isDirectory(contentPath)) {
            Thread.sleep(1000);
        }

        WatchService contentWatchService = FileSystems.getDefault().newWatchService();
        contentPath.register(contentWatchService, StandardWatchEventKinds.ENTRY_CREATE);

        Thread contentWatcherThread = new Thread(() -> {
            try {
                while (true) {
                    WatchKey contentKey;
                    try {
                        contentKey = contentWatchService.take();
                    } catch (InterruptedException ex) {
                        return;
                    }

                    for (WatchEvent<?> contentEvent : contentKey.pollEvents()) {
                        WatchEvent.Kind<?> contentKind = contentEvent.kind();
                        if (contentKind == StandardWatchEventKinds.ENTRY_CREATE) {
                            @SuppressWarnings("unchecked")
                            WatchEvent<Path> contentEv = (WatchEvent<Path>) contentEvent;
                            Path contentFilename = contentEv.context();
                            Path contentTotalPath = contentPath.resolve(contentFilename);
                            System.out.println("File Created in Content Directory: " + contentTotalPath);
                            task(bookFolder);
                        }
                    }

                    boolean valid = contentKey.reset();
                    if (!valid) {
                        break;
                    }
                }
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
        });

        contentWatcherThread.start();
    }


    private void task(Path filename) throws SQLException, IOException {
        taskDelete();
        Map<Book, Map<Associate, Word>> books = datalake.read(filename.toFile(), datamart.getMaxId());
        System.out.println("Filename: ");
        System.out.println(filename.toFile());
        for (Map.Entry<Book, Map<Associate, Word>> entry : books.entrySet()) {
            Book book = entry.getKey();
            Map<Associate, Word> bookData = entry.getValue();
            datamart.addBook(book);

            for (Map.Entry<Associate, Word> dataEntry : bookData.entrySet()) {
                Associate associate = dataEntry.getKey();
                Word word = dataEntry.getValue();

                Word existingWord = datamart.findWordByLabel(word.getLabel());
                if (existingWord != null) {
                    associate.setWordId(existingWord.getId());
                } else {
                    datamart.addWord(word);
                }
                datamart.addAssociation(associate);
            }
        }
    }

    private void taskDelete() throws SQLException {
        datamart.initDatabase();
    }
}
