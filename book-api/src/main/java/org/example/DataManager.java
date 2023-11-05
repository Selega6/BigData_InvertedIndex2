package org.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class DataManager implements Command {
    private final DatabaseReader databaseReader;
    private static final String ALL_BOOKS = "SELECT * FROM book";
    private static final String ALL_WORDS = "SELECT * FROM word";
    private static final String ALL_ASSOCIATIONS = "SELECT * FROM associate";

    public DataManager() throws SQLException {
        this.databaseReader = new SqliteReader();
    }

    @Override
    public List<Book> getAllBooks() throws SQLException {
        return databaseReader.readBooks(ALL_BOOKS);
    }

    @Override
    public List<Word> getAllWords() throws SQLException {
        return databaseReader.readWords(ALL_WORDS);
    }

    @Override
    public List<Associate> getAllAssociations() throws SQLException {
        return databaseReader.readAssociations(ALL_ASSOCIATIONS);
    }

    public boolean updateBook(int bookId, Book updatedBook) throws SQLException {
        String sql = "UPDATE book SET author = ?, title = ? WHERE id = ?";
        return databaseReader.update(sql, updatedBook.getAuthor(), updatedBook.getTitle(), bookId);
    }

    public boolean deleteBook(int bookId) throws SQLException {
        String sql = "DELETE FROM book WHERE id = ?";
        return databaseReader.delete(sql, bookId);
    }

    public int createBook(Book newBook) throws SQLException {
        String sql = "INSERT INTO book (author, title) VALUES (?, ?)";
        return databaseReader.create(sql,newBook.getAuthor(),newBook.getTitle());
    }

    public List<Book> searchBooks(String searchTerm) throws SQLException {
        SqliteReader sqliteReader = new SqliteReader();
        Connection connection = sqliteReader.getConnection();
        String searchQuery = "SELECT book.* FROM book " +
                "JOIN associate ON book.id = associate.bookId " +
                "JOIN word ON associate.wordId = word.id " +
                "WHERE word.label LIKE ? GROUP BY book.id";

        try (PreparedStatement stmt = connection.prepareStatement(searchQuery)) {
            stmt.setString(1, "%" + searchTerm + "%");

            ResultSet rs = stmt.executeQuery();
            List<Book> searchResults = new ArrayList<>();

            while (rs.next()) {
                Book book = new Book(rs.getInt("id"), rs.getString("author"), rs.getString("title"));
                searchResults.add(book);
            }

            return searchResults;
        }
    }
}
