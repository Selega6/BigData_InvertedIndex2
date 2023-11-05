package org.example;

public class SqliteWriter {
    private static final String INSERT_BOOK =
            "INSERT INTO book(id, author, title) VALUES (%d, '%s', '%s')";
    private static final String INSERT_WORD =
            "INSERT INTO word(id, label) VALUES (%d, '%s')";
    private static final String INSERT_ASSOCIATE =
            "INSERT INTO associate(wordId, bookId, count) VALUES ('%d', '%d', '%d')";
    public static String insertBookStatementOf(Book book) {
        System.out.println(book.getAuthor());
        return String.format(INSERT_BOOK,
                book.getId(),
                book.getAuthor(),
                book.getTitle());
    }

    public static String insertWordStatementOf(Word word) {
        return String.format(INSERT_WORD,
                word.getId(),
                word.getLabel());
    }

    public static String insertAssociateStatementOf(Associate associate) {
        return String.format(INSERT_ASSOCIATE,
                associate.getWordId(),
                associate.getBookId(),
                associate.getCount());
    }
}
