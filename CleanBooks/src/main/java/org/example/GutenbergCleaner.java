package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GutenbergCleaner implements BookCleaner{

    @Override
    public String cleanText(String content) {
        String lowercaseString = content.toLowerCase();
        List<String> stopWordsList  = readWords("CleanBooks//src/main//resources//StopWords.txt");

        String cleanContent = lowercaseString.replaceAll("[^\\p{L}\\p{Nd}]+", " ");
        cleanContent = cleanContent.replaceAll("\\d","");
        cleanContent = cleanContent.trim().replaceAll("\t", " ");
        cleanContent = cleanContent.trim().replaceAll("\n", " ");

        for(String word : stopWordsList){
            cleanContent=cleanContent.replaceAll("\\s"+word+"\\s"," ");
        }

        String removeSingleLettersRegex = "\\s[a-zA-Z]\\s";
        cleanContent = cleanContent.replaceAll(removeSingleLettersRegex, " ");

        String removeConsonantWordsRegex = "\\b[bcdfghjklmnpqrstvwxyz]+\\b";
        cleanContent = cleanContent.replaceAll(removeConsonantWordsRegex, "");
        cleanContent =  cleanContent.trim().replaceAll("\\s{1,}", " ");

        return cleanContent;
    }

    @Override
    public List<String> readWords(String path) {
        List<String> wordsList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.split(" ");
                for (String word : words) {
                    wordsList.add(word.trim());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return wordsList;
    }
}
