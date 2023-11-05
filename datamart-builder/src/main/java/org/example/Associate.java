package org.example;

public class Associate {
    public void setWordId(int wordId) {
        this.wordId = wordId;
    }

    int wordId;
    int bookId;
    int count;
    public Associate(int wordId, int bookId, int count) {
        this.wordId = wordId;
        this.bookId = bookId;
        this.count = count;
    }

    public int getWordId() {
        return wordId;
    }

    public int getBookId() {
        return bookId;
    }

    public int getCount() {
        return count;
    }
}
