package org.example;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FileDatalake implements Datalake{


    public static int countWordOccurrences(List<String> words, String word) {
        return (int) words.stream()
                .filter(w -> w.equals(word))
                .count();
    }
    public static Map<Associate, Word> invertedIndexFromFilesWithCount(String booksDirectory, Book book, int wordMaxId) {
        Map<Associate, Word> invertedIndex = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(booksDirectory))) {
            List<String> document = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                document.addAll(Arrays.asList(line.split("\\s+")));
            }
            Set<String> uniqueWords = new HashSet<>(document);
                        int add = 1;
                        for (String word : uniqueWords) {
                            int wordId = wordMaxId+add;
                            Word newWord = new Word(wordId, word);
                            Associate associate = new Associate(newWord.getId(), book.getId(), countWordOccurrences(document, word));
                            invertedIndex.put(associate, newWord);
                            add+=1;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

        return invertedIndex;
    }

    @Override
    public Map<Book, Map<Associate, Word>> read(File directory, int wordMaxId) throws IOException {
        Map<Book, Map<Associate, Word>> result = new HashMap<>();

                Path path = Paths.get(directory.toString());
                String lastSegment = path.getFileName().toString();
                File metadata = new File(path + "/metadata/" + lastSegment + ".json");
                BufferedReader br = new BufferedReader(new FileReader(metadata));
                String line;

                Book book = null;
                while ((line = br.readLine()) != null) {
                    JsonObject jsonObjet = new Gson().fromJson(line, JsonObject.class);
                    String author = "Anonymous";
                    if (jsonObjet.getAsJsonPrimitive("Author") != null){
                        author = jsonObjet.getAsJsonPrimitive("Author").getAsString().replace("'", "");
                    }
                    String title = "Untitle";
                    if (jsonObjet.getAsJsonPrimitive("Title") != null){
                        title = jsonObjet.getAsJsonPrimitive("Title").getAsString().replace("'", "");
                    }
                    int id = Integer.valueOf(lastSegment);
                    book = new Book(id, author, title);

                }
                Map<Associate, Word> invertedIndex = invertedIndexFromFilesWithCount(directory + "/content/" + lastSegment + ".txt", book, wordMaxId);
                result.put(book, invertedIndex);
        return result;
    }

}
